// promdump fetches time series points for a given metric from Prometheus server
// and saves them into a series of json files. Files contain a serialized list of
// SampleStream messages (see model/value.go).
// Generated files can later be read by promremotewrite which will write the
// points to a Prometheus remote storage.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

const appVersion = "0.1.2"
const defaultPeriod = 7 * 24 * time.Hour // 7 days
const defaultBatchDuration = 24 * time.Hour

type promExport struct {
	exportName string
	collect    bool
	isDefault  bool
}

var (
	// Also see init() below for aliases
	version        = flag.Bool("version", false, "prints the promdump version and exits")
	baseURL        = flag.String("url", "http://localhost:9090", "URL for Prometheus server API")
	startTime      = flag.String("start_time", "", "timestamp to start querying at (RFC3339, e.g. 2023-03-13T01:00:00-0100).")
	endTime        = flag.String("end_time", "", "timestamp to end querying at (RFC3339). Defaults to current time.")
	periodDur      = flag.Duration("period", 0, "time period to get data for")
	batchDur       = flag.Duration("batch", defaultBatchDuration, "batch size: time period for each query to Prometheus server.")
	metric         = flag.String("metric", "", "custom metric to fetch (optional; can include label values)")
	out            = flag.String("out", "", "output file prefix; only used for custom -metric specifications")
	nodePrefix     = flag.String("node_prefix", "", "node prefix value for Yugabyte Universe, e.g. yb-prod-appname")
	batchesPerFile = flag.Uint("batches_per_file", 1, "batches per output file")

	// Whether to collect node_export, master_export, tserver_export, etc; see init() below for implementation
	collectMetrics = map[string]*promExport{
		"master":  {exportName: "master_export", collect: true, isDefault: true},
		"node":    {exportName: "node_export", collect: true, isDefault: true},
		"tserver": {exportName: "tserver_export", collect: true, isDefault: true},
		// nb: cql_export is not a typo
		// TODO: Maybe add a "cql" alias but if we do that, we need to squash duplicates
		"ycql": {exportName: "cql_export", collect: true, isDefault: true},
		"ysql": {exportName: "ysql_export", collect: true, isDefault: true},
	}
)

func init() {
	flag.BoolVar(version, "v", false, "prints the promdump version and exits")
	flag.StringVar(endTime, "timestamp", "", "alias for end_time")
	// Process CLI flags for collection of YB prometheus exports (master, node, tserver, ycql, ysql)
	for k, v := range collectMetrics {
		// Needed to break closure
		k := k
		v := v
		flag.Func(k, fmt.Sprintf("collect metrics for %v", v.exportName), func(s string) error {
			var err error
			v.collect, err = strconv.ParseBool(s)
			v.isDefault = false
			return err
		})
	}
}

func logMetricCollectorConfig() {
	// Logs the collector config (
	var collect []string
	var skip []string
	for _, v := range collectMetrics {
		if *out != "" && *out == v.exportName {
			log.Fatalln(fmt.Sprintf("The output file prefix '%v' is reserved. Specify a different --out value.", v.exportName))
		}
		if v.collect {
			collect = append(collect, v.exportName)
		} else {
			skip = append(skip, v.exportName)
		}
	}
	if len(collect) > 0 {
		sort.Strings(collect)
		log.Printf("main: collecting the following Yugabyte metrics: %v", strings.Join(collect, ", "))
	}
	if len(skip) > 0 {
		sort.Strings(skip)
		log.Printf("main: skipping the following Yugabyte metrics: %v", strings.Join(skip, ", "))
	}
	if *metric != "" {
		log.Printf("main: collecting the following custom metric: '%v'", *metric)
	}
}

// dump a slice of SampleStream messages to a json file.
func writeFile(values *[]*model.SampleStream, filePrefix string, fileNum uint) error {
	/*
	   This check is duplicated here because we call writeFile unconditionally to flush any pending writes before
	   exiting and we don't want to print a stray "Writing 0 results to file" line.
	*/
	if len(*values) == 0 {
		return nil
	}
	filename := fmt.Sprintf("%s.%05d", filePrefix, fileNum)
	valuesJSON, err := json.Marshal(values)
	if err != nil {
		return err
	}
	log.Printf("writeFile: writing %v results to file %v", len(*values), filename)
	return ioutil.WriteFile(filename, valuesJSON, 0644)
}

func cleanFiles(filePrefix string, fileNum uint) (uint, error) {
	for i := uint(0); i < fileNum; i++ {
		filename := fmt.Sprintf("%s.%05d", filePrefix, i)
		err := os.Remove(filename)
		if err != nil {
			// If removal of the first file fails, we have removed 0 files.
			log.Printf("cleanFiles: %v stale output file(s) removed. Removal of file %v failed.", i, filename)
			return i, err
		}
	}
	if fileNum > 0 {
		log.Printf("cleanFiles: %v stale output file(s) removed.", fileNum)
	}
	return fileNum, nil
}

func getRangeTimestamps(startTime string, endTime string, period time.Duration) (time.Time, time.Time, time.Duration, error) {
	var err error
	var startTS, endTS time.Time

	err = nil

	badTime := time.Time{}

	if startTime != "" && endTime != "" && period != 0 {
		return badTime, badTime, 0, errors.New("only two of start time, end time, and duration may be specified when calculating Prometheus query range")
	}

	// If neither the start time nor end time are specified, use the default end time for backward compatibility
	if startTime == "" && endTime == "" {
		endTS = time.Now()
	}

	// Parse any provided time strings into Go times
	if startTime != "" {
		startTS, err = time.Parse(time.RFC3339, startTime)
		if err != nil {
			return badTime, badTime, 0, err
		}
	}
	if endTime != "" {
		endTS, err = time.Parse(time.RFC3339, endTime)
		if err != nil {
			return badTime, badTime, 0, err
		}
	}

	// If the caller did not provide a period, we need to calculate it if possible or use the default if not
	if period == 0 {
		if startTime != "" && endTime != "" {
			// If both start time and end time are specified, the period is the difference
			period = endTS.Sub(startTS)
		} else {
			// In all other cases, the period should be the default
			period = defaultPeriod
		}
	}

	// When we reach this point, we are guaranteed to have one timestamp and the period,
	// so calculate the other timestamp if needed
	if startTS.IsZero() {
		startTS = endTS.Add(-period)
	} else if endTS.IsZero() {
		endTS = startTS.Add(period)
	}

	if startTS.After(time.Now()) {
		return badTime, badTime, 0, errors.New("start time must be in the past")
	}

	if startTS.After(endTS) {
		return badTime, badTime, 0, errors.New("start time is after end time, which is not permitted")
	}

	// Don't query past the current time because that would be dumb
	if endTS.After(time.Now()) {
		endTS = time.Now()
	}

	return startTS, endTS, period, nil
}

func exportMetric(ctx context.Context, promApi v1.API, metric string, beginTS time.Time, endTS time.Time, periodDur time.Duration, batchDur time.Duration, batchesPerFile uint, filePrefix string) error {
	allBatchesFetched := false

	values := make([]*model.SampleStream, 0, 0)
	fileNum := uint(0)
	// There's no way to restart a loop iteration in golang, so apply brute force and ignorance to batch size backoff
	for allBatchesFetched == false {
		batches := uint(math.Ceil(periodDur.Seconds() / batchDur.Seconds()))
		if batches > 99999 {
			return errors.New(fmt.Sprintf("batch settings could generate %v batches, which is an unreasonable number of batches", batches))
		}
		log.Printf("exportMetric: querying from %v to %v in %v batches\n", beginTS, endTS, batches)
		for batch := uint(1); batch <= batches; batch++ {
			// TODO: Refactor this into getBatch()?
			queryTS := beginTS.Add(batchDur * time.Duration(batch))
			lookback := batchDur.Seconds()
			if queryTS.After(endTS) {
				lookback -= queryTS.Sub(endTS).Seconds()
				queryTS = endTS
			}

			query := fmt.Sprintf("%s[%ds]", metric, int64(lookback))
			log.Printf("exportMetric: querying %s ending at timestamp %v", query, queryTS.Format(time.RFC3339))
			// TODO: Add support for overriding the timeout; remember it can only go *smaller*
			value, _, err := promApi.Query(ctx, query, queryTS)

			if err != nil {
				// This is horrible but the golang prometheus_client swallows the 422 HTTP return code, so we have to
				// scrape instead :(
				tooManySamples, _ := regexp.Match("query processing would load too many samples into memory", []byte(err.Error()))
				if tooManySamples {
					newBatchDur := time.Duration(batchDur.Nanoseconds() / 2)
					log.Printf("exportMetric: too many samples in result set. Reducing batch size from %v to %v and trying again.", batchDur, newBatchDur)
					_, err := cleanFiles(filePrefix, fileNum)
					fileNum = 0
					if err != nil {
						return errors.New(fmt.Sprintf("failed to clean up stale export files: %s", err))
					}
					batchDur = newBatchDur
					if batchDur.Seconds() <= 1 {
						return errors.New(fmt.Sprintf("failed to query Prometheus for metric %v - too much data even at minimum batch size", metric))
					}
					break
				} else {
					return err
				}
			}

			if value == nil {
				return errors.New(fmt.Sprintf("metric %v returned an invalid result set", metric))
			}

			if value.Type() != model.ValMatrix {
				return errors.New(fmt.Sprintf("when querying metric %v, expected return value to be of type matrix; got type %v instead", metric, value.Type()))
			}
			// model/value.go says: type Matrix []*SampleStream
			values = append(values, value.(model.Matrix)...)

			if batch%batchesPerFile == 0 {
				if len(values) > 0 {
					err = writeFile(&values, filePrefix, fileNum)
					if err != nil {
						return errors.New(fmt.Sprintf("batch write failed with: %v", err))
					}
					fileNum++
					values = make([]*model.SampleStream, 0, 0)
				} else {
					log.Println("exportMetric: no results for this query, skipping write")
				}
			}
			// If this is the last batch, exit the outer loop
			if batch == batches {
				allBatchesFetched = true
			}
		}
	}
	return writeFile(&values, filePrefix, fileNum)
}

func getBatch(ctx context.Context, promApi v1.API, metric string, beginTS time.Time, endTS time.Time, periodDur time.Duration, batchDur time.Duration) ([]*model.SampleStream, error) {
	// TODO: Refactor to use this func or get rid of it
	return nil, nil
}

func main() {
	flag.Parse()

	if *version {
		fmt.Printf("promdump version %v\n", appVersion)
		os.Exit(0)
	}

	log.Printf("Starting promdump version %v\n", appVersion)

	if *nodePrefix == "" && *metric == "" {
		log.Fatalln("Specify a --node_prefix value (if collecting default Yugabyte metrics), a custom metric using --metric, or both.")
	}
	if *nodePrefix == "" && *metric != "" {
		// If the user has not provided a node prefix but has provided a metric, flip default yb metrics
		// collection off.
		for _, v := range collectMetrics {
			v := v
			if v.isDefault {
				v.collect = false
			}
			if *nodePrefix == "" && v.collect == true {
				log.Fatalln("Specify a --node_prefix value or remove any Yugabyte metric export collection flags (--master, --node, etc.)")
			}
		}
	}

	if *metric != "" && *out == "" {
		log.Fatalln("When specifying a custom --metric, output file prefix --out is required.")
	}

	logMetricCollectorConfig()

	if *startTime != "" && *endTime != "" && *periodDur != 0 {
		log.Fatalln("Too many time arguments. Specify either --start_time and --end_time or a time and --period.")
	}

	var beginTS, endTS time.Time
	var err error
	beginTS, endTS, *periodDur, err = getRangeTimestamps(*startTime, *endTime, *periodDur)
	if err != nil {
		log.Fatalln("getRangeTimeStamps: ", err)
	}

	// This check has moved below timestamp calculations because the period may now be a calculated value
	if periodDur.Nanoseconds()%1e9 != 0 || batchDur.Nanoseconds()%1e9 != 0 {
		log.Fatalln("--period and --batch must not have fractional seconds")
	}
	if *batchDur > *periodDur {
		batchDur = periodDur
	}

	ctx := context.Background()
	client, err := api.NewClient(api.Config{Address: *baseURL})
	if err != nil {
		log.Fatalln("api.NewClient:", err)
	}
	promApi := v1.NewAPI(client)

	// TODO: DRY this out
	var fileConflictPrefixes []string
	if *nodePrefix != "" {
		for _, v := range collectMetrics {
			matches, err := filepath.Glob(fmt.Sprintf("%s.[0-9][0-9][0-9][0-9][0-9]", v.exportName))
			if err != nil {
				log.Fatalln(fmt.Sprintf("main: checking for existing export files with file prefix %v failed with error: %v", v.exportName, err))
			}
			if len(matches) > 0 {
				// No error = file already exists
				fileConflictPrefixes = append(fileConflictPrefixes, v.exportName+".*")
			}
		}
	}
	if *out != "" {
		matches, err := filepath.Glob(fmt.Sprintf("%s.[0-9][0-9][0-9][0-9][0-9]", *out))
		if err != nil {
			log.Fatalln(fmt.Sprintf("main: checking for existing export files with file prefix %v failed with error: %v", *out, err))
		}
		if len(matches) > 0 {
			// No error = file already exists
			fileConflictPrefixes = append(fileConflictPrefixes, *out+".*")
		}
	}
	if len(fileConflictPrefixes) > 0 {
		sort.Strings(fileConflictPrefixes)
		log.Fatalln(fmt.Sprintf("main: found existing export files with file prefix(es): %v; move any existing export files aside before proceeding", strings.Join(fileConflictPrefixes, " ")))
	}

	// TODO: DRY this out
	// Loop through yb metrics list and export each metric according to its configuration
	for _, v := range collectMetrics {
		if v.collect {
			ybMetric := fmt.Sprintf("{export_type=\"%s\",node_prefix=\"%s\"}", v.exportName, *nodePrefix)
			err = exportMetric(ctx, promApi, ybMetric, beginTS, endTS, *periodDur, *batchDur, *batchesPerFile, v.exportName)
			if err != nil {
				log.Printf("exportMetric: export of metric %v failed with error %v; moving to next metric", v.exportName, err)
				continue
			}
		}
	}
	if *metric != "" {
		err = exportMetric(ctx, promApi, *metric, beginTS, endTS, *periodDur, *batchDur, *batchesPerFile, *out)
		if err != nil {
			log.Fatalln("exportMetric:", err)
		}
	}
}
