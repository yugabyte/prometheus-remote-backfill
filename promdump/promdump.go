// promdump fetches time series points for a given metric from Prometheus server
// and saves them into a series of json files. Files contain a serialized list of
// SampleStream messages (see model/value.go).
// Generated files can later be read by promremotewrite which will write the
// points to a Prometheus remote storage.
package main

import (
	"archive/tar"

	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dsnet/compress/bzip2"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

const defaultPeriod = 7 * 24 * time.Hour // 7 days
const defaultBatchDuration = 24 * time.Hour

type promExport struct {
	exportName         string
	jobName            string
	collect            bool
	changedFromDefault bool
	requiresNodePrefix bool
	fileCount          uint
}

var (
	// Also see init() below for aliases
	debugLogging     = flag.Bool("debug", false, "enable additional debug logging")
	version          = flag.Bool("version", false, "prints the promdump version and exits")
	baseURL          = flag.String("url", "http://localhost:9090", "URL for Prometheus server API")
	startTime        = flag.String("start_time", "", "RFC3339 `timestamp` to start querying at (e.g. 2023-03-13T01:00:00-0100).")
	endTime          = flag.String("end_time", "", "RFC3339 `timestamp` to end querying at (default now)")
	periodDur        = flag.Duration("period", 0, "time period to get data for")
	batchDur         = flag.Duration("batch", defaultBatchDuration, "batch size: time period for each query to Prometheus server.")
	metric           = flag.String("metric", "", "custom metric to fetch (optional; can include label values)")
	out              = flag.String("out", "", "output file prefix; only used for custom --metric specifications")
	nodePrefix       = flag.String("node_prefix", "", "node prefix value for Yugabyte Universe, e.g. yb-prod-appname")
	prefixValidation = flag.Bool("node_prefix_validation", true, "set to false to disable node prefix validation")
	instanceList     = flag.String("instances", "", "the instance name(s) for which to collect metrics (optional, mutually exclusive with --nodes; comma separated list, e.g. yb-prod-appname-n1,yb-prod-appname-n3,yb-prod-appname-n4,yb-prod-appname-n5,yb-prod-appname-n6,yb-prod-appname-n14; disables collection of platform metrics unless explicitly enabled with --platform")
	nodeSet          = flag.String("nodes", "", "the node number(s) for which to collect metrics (optional, mutually exclusive with --instances); comma separated list of node numbers or ranges, e.g. 1,3-6,14; disables collection of platform metrics unless explicitly requested with --platform")
	batchesPerFile   = flag.Uint("batches_per_file", 1, "batches per output file")
	enableTar        = flag.Bool("tar", true, "enable bundling exported metrics into a tar file")
	tarCompression   = flag.String("tar_compression_algorithm", "none", "compression algorithm to use when creating a tar bundle; one of \"gzip\", \"bzip2\", or \"none\"")
	tarFilename      = flag.String("tar_filename", "", "filename for the generated tar file")

	// Whether to collect node_export, master_export, tserver_export, etc; see init() below for implementation
	collectMetrics = map[string]*promExport{
		// collect: collect this by default (true/false)
		// changedFromDefault: flag has been overridden (placeholder set at runtime - leave false)
		"master":     {exportName: "master_export", collect: true, changedFromDefault: false, requiresNodePrefix: true},
		"node":       {exportName: "node_export", collect: true, changedFromDefault: false, requiresNodePrefix: true},
		"platform":   {jobName: "platform", collect: true, changedFromDefault: false, requiresNodePrefix: false},
		"prometheus": {jobName: "prometheus", collect: false, changedFromDefault: false, requiresNodePrefix: false},
		"tserver":    {exportName: "tserver_export", collect: true, changedFromDefault: false, requiresNodePrefix: true},
		// nb: cql_export is not a typo
		// TODO: Maybe add a "cql" alias but if we do that, we need to squash duplicates
		"ycql": {exportName: "cql_export", collect: true, changedFromDefault: false, requiresNodePrefix: true},
		"ysql": {exportName: "ysql_export", collect: true, changedFromDefault: false, requiresNodePrefix: true},
	}

	AppVersion = "DEV BUILD"
	CommitHash = "POPULATED_BY_BUILD"
	BuildTime  = "POPULATED_BY_BUILD"
)

func init() {
	flag.BoolVar(version, "v", false, "prints the promdump version and exits")
	flag.StringVar(endTime, "timestamp", "", "alias for end_time (`timestamp`)")
	// Process CLI flags for collection of YB prometheus exports (master, node, tserver, ycql, ysql)
	for k, v := range collectMetrics {
		// Needed to break closure
		k := k
		v := v

		metricName, err := getMetricName(v)
		if err != nil {
			log.Fatalf("init: %v", err)
		}

		// Backticks set the type string for flags in --help output
		flag.Func(k, fmt.Sprintf("``collect metrics for %v (default %v)", metricName, v.collect), func(s string) error {
			var err error
			v.collect, err = strconv.ParseBool(s)
			v.changedFromDefault = true
			return err
		})
	}
}

func getMetricName(metric *promExport) (string, error) {
	if metric.exportName != "" && metric.jobName != "" {
		return "", errors.New("getMetricName: exportName and jobName are mutually exclusive")
	}

	if metric.exportName != "" {
		return metric.exportName, nil
	} else if metric.jobName != "" {
		return metric.jobName, nil
	}
	return "", errors.New("getMetricName: no metric name fields found, unable to determine metric name")
}

func logMetricCollectorConfig() {
	// Logs the collector config
	var collect []string
	var skip []string
	for _, v := range collectMetrics {

		metricName, err := getMetricName(v)
		if err != nil {
			log.Fatalf("logMetricCollectorConfig: %v", err)
		}

		if *out != "" && *out == metricName {
			log.Fatalf("The output file prefix '%v' is reserved. Specify a different --out value.", metricName)
		}
		if v.collect {
			collect = append(collect, metricName)
		} else {
			skip = append(skip, metricName)
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
	if *debugLogging {
		log.Printf("writeFile: writing %v results to file %v", len(*values), filename)
	}
	return os.WriteFile(filename, valuesJSON, 0644)

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
		curTS := time.Now()
		log.Printf("getRangeTimestamps: end time %v is after current time %v; setting end time to %v and recalculating period", endTS.Format(time.RFC3339), curTS.Format(time.RFC3339), curTS.Format(time.RFC3339))
		endTS = curTS
		/*
		   Recalculate the period and round to the nearest second.

		   We may lose a single sample here by not rounding up but I'm not doing a bunch of annoying type conversions
		   for a 1/15 chance to lose one sample that we probably don't care about anyway.
		*/
		period = endTS.Sub(startTS).Round(time.Second)
	}

	if *debugLogging {
		log.Printf("getRangeTimestamps: returning start time %v, end time %v, and period %v", startTS.Format(time.RFC3339), endTS.Format(time.RFC3339), period)
	}
	return startTS, endTS, period, nil
}

func writeErrIsFatal(err error) bool {
	// Handle fatal golang "portable" errors
	if errors.Is(err, os.ErrPermission) {
		// Permission denied errors
		return true
	} else if errors.Is(err, os.ErrExist) || errors.Is(err, os.ErrNotExist) {
		// Errors if a file unexpectedly exists or doesn't exist
		return true
	}

	// Handle fatal OS-specific errors
	// nb: Checking ENOSPC may not work Windows but should work on all other supported OSes
	if runtime.GOOS != "windows" && errors.Is(err, syscall.ENOSPC) {
		// Out of disk space
		return true
	}

	return false
}
func exportMetric(ctx context.Context, promApi v1.API, metric string, beginTS time.Time, endTS time.Time, periodDur time.Duration, batchDur time.Duration, batchesPerFile uint, filePrefix string) (uint, error) {
	allBatchesFetched := false
	if *debugLogging {
		log.Printf("exportMetric: received start time %v, end time %v, and period %v", beginTS.Format(time.RFC3339), endTS.Format(time.RFC3339), periodDur)
	}

	values := make([]*model.SampleStream, 0, 0)
	fileNum := uint(0)
	// There's no way to restart a loop iteration in golang, so apply brute force and ignorance to batch size backoff
	for allBatchesFetched == false {
		batches := uint(math.Ceil(periodDur.Seconds() / batchDur.Seconds()))
		if batches > 99999 {
			return 0, fmt.Errorf("batch settings could generate %v batches, which is an unreasonable number of batches", batches)
		}
		log.Printf("exportMetric: querying metric '%v' from %v to %v in %v batches\n", metric, beginTS.Format(time.RFC3339), endTS.Format(time.RFC3339), batches)
		for batch := uint(1); batch <= batches; batch++ {
			// TODO: Refactor this into getBatch()?
			queryTS := beginTS.Add(batchDur * time.Duration(batch))
			lookback := batchDur.Seconds()
			if queryTS.After(endTS) {
				lookback -= queryTS.Sub(endTS).Seconds()
				queryTS = endTS
			}

			query := fmt.Sprintf("%s[%ds]", metric, int64(lookback))
			if *debugLogging {
				log.Printf("exportMetric: executing query '%s' ending at timestamp %v", query, queryTS.Format(time.RFC3339))
			} else {
				log.Printf("exportMetric: batch %v/%v (to %v)", batch, batches, queryTS.Format(time.Stamp))
			}
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
						return 0, fmt.Errorf("failed to clean up stale export files: %w", err)
					}
					batchDur = newBatchDur
					if batchDur.Seconds() <= 1 {
						return 0, fmt.Errorf("failed to query Prometheus for metric %v - too much data even at minimum batch size", metric)
					}
					break
				} else {
					return 0, err
				}
			}

			if value == nil {
				return 0, fmt.Errorf("metric %v returned an invalid result set", metric)
			}

			if value.Type() != model.ValMatrix {
				return 0, fmt.Errorf("when querying metric %v, expected return value to be of type matrix; got type %v instead", metric, value.Type())
			}
			// model/value.go says: type Matrix []*SampleStream
			values = append(values, value.(model.Matrix)...)

			if batch%batchesPerFile == 0 {
				if len(values) > 0 {
					err = writeFile(&values, filePrefix, fileNum)
					if err != nil {
						batchErr := fmt.Errorf("batch write failed with '%w'", err)
						if writeErrIsFatal(batchErr) {
							log.Fatalf("exportMetric: %v, giving up", batchErr)
						}
						return 0, batchErr
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

	return fileNum, writeFile(&values, filePrefix, fileNum)

}

// Holds custom metric file counts
var customMetricCount uint

func createArchive(buf io.Writer) error {
	// Create new Writers for gzip , bzip2 and none(tar)

	var tw *tar.Writer

	switch *tarCompression {
	case "gzip":
		gw := gzip.NewWriter(buf)
		defer gw.Close()
		tw = tar.NewWriter(gw)

	case "bzip2":
		bw, err := bzip2.NewWriter(buf, nil)
		if err != nil {
			return err
		}
		defer bw.Close()
		tw = tar.NewWriter(bw)

	case "none":
		tw = tar.NewWriter(buf)
	default:
		fmt.Errorf("unsupported compression type: %s , using default tar_compression_algorithm", *tarCompression)

	}
	defer tw.Close()

	// Iterate over collectMetrics and add them to the tar archive
	for _, v := range collectMetrics {
		for i := 0; i < int(v.fileCount); i++ {
			metricName, err := getMetricName(v)
			if err != nil {
				log.Fatalf("createArchive: %v", err)
			}
			filename := fmt.Sprintf("%s.%05d", metricName, i)
			err = addToArchive(tw, filename)
			if err != nil {
				return err
			}

		}
	}
	//custom filename collect and add them to the tar archive
	if *out != "" {
		for i := 0; i < int(customMetricCount); i++ {
			filename := fmt.Sprintf("%s.%05d", *out, i)
			err := addToArchive(tw, filename)
			if err != nil {
				return err
			}

		}

	}

	return nil
}

func addToArchive(tw *tar.Writer, filename string) error {
	// Open the file which will be written into the archive
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get FileInfo about our file providing file size, mode, etc.
	info, err := file.Stat()
	if err != nil {
		return err
	}

	// Create a tar Header from the FileInfo data
	header, err := tar.FileInfoHeader(info, info.Name())
	if err != nil {
		return err
	}

	// Write file header to the tar archive
	if err := tw.WriteHeader(header); err != nil {
		return err
	}

	// Copy file content to tar archive
	_, err = io.Copy(tw, file)
	if err != nil {
		return err
	}

	return nil
}

func generateDefaultTarFilename() string {
	compressionType := *tarCompression

	switch compressionType {
	case "gzip":
		return fmt.Sprintf("promdump-%s-%s.tar.gz", *nodePrefix, time.Now().Format("20060102-150405")) // Format: YYYYMMDD-HHMMSS
	case "bzip2":
		return fmt.Sprintf("promdump-%s-%s.tar.bz2", *nodePrefix, time.Now().Format("20060102-150405"))

	}
	return fmt.Sprintf("promdump-%s-%s.tar", *nodePrefix, time.Now().Format("20060102-150405"))

}

func getBatch(ctx context.Context, promApi v1.API, metric string, beginTS time.Time, endTS time.Time, periodDur time.Duration, batchDur time.Duration) ([]*model.SampleStream, error) {
	// TODO: Refactor to use this func or get rid of it
	return nil, nil
}

func hasConflictingFiles(filename string) (bool, error) {
	matches, err := filepath.Glob(fmt.Sprintf("%s.[0-9][0-9][0-9][0-9][0-9]", filename))
	if err != nil {
		return true, err
	}
	return len(matches) > 0, nil
}

func buildInstanceLabelString(instanceList string, nodeSet string) (string, error) {
	instanceLabelString := ""
	if instanceList != "" {
		// This branch of the if converts a comma separated list of instances into a regular expression that will match
		// against each of the instance names, e.g. instance1,instance2 => (?:instance1|instance2)

		// Using a CSV library here is *probably* overkill but it's not any harder to read and should save us grief if
		// there are any weird characters in the input.
		r := csv.NewReader(strings.NewReader(instanceList))
		instances, err := r.Read()
		if err != nil {
			return "", fmt.Errorf("failed to parse --instances flag: %w", err)
		}
		/*
			The extra | before the list of instance names is required so we capture certain metrics like "up"
			that can't be relabeled and therefore do not have an exported_instance label. This may result in collecting
			duplicates for these metrics but dupes are the lesser of two evils in this case since the "up" metric
			is extremely useful for troubleshooting certain classes of issues.
		*/
		// TODO: Warn if the instances don't start with the same substring?
		// TODO: Validate the instance list to make sure all the instances end with a node number?
		instanceLabelString = fmt.Sprintf("exported_instance=~\"(?:|%v)\"", strings.Join(instances, "|"))
	} else if nodeSet != "" {
		// This branch of the if converts a comma separated list of node numbers or ranges of node numbers into a
		// regular expression that will match against node names, e.g. 1,3-7,12 => yb-dev-univname-n(?:1|3|4|5|6|7|12)

		// Using a CSV library here is *probably* overkill but it's not any harder to read and should save us grief if
		// there are any weird characters in the input.
		r := csv.NewReader(strings.NewReader(nodeSet))
		fields, err := r.Read()
		if err != nil {
			return "", fmt.Errorf("failed to parse --nodes flag: %w", err)
		}
		// Matches individual node numbers (natural numbers)
		nodeNumRe := regexp.MustCompile(`^(\d+)$`)
		// Matches ranges of the format a-c (where a and c are arbitrary natural numbers)
		rangeRe := regexp.MustCompile(`^([1-9][0-9]*)-([1-9][0-9]*)$`)
		var nodes []string
		for _, v := range fields {
			if *debugLogging {
				log.Printf("buildInstanceLabelString: found field '%v' in --nodes flag", v)
			}
			rangeMatches := rangeRe.FindStringSubmatch(v)

			if nodeNumRe.FindStringSubmatch(v) != nil {
				// We'll hit this branch if we find an individual node number in a field
				if *debugLogging {
					log.Printf("buildInstanceLabelString: adding node n%v to the node list", v)
				}
				nodes = append(nodes, v)
			} else if len(rangeMatches) == 3 {
				// We'll hit this branch if we matched the range regular expression rangeRe above, e.g. 3-7
				if *debugLogging {
					log.Printf("buildInstanceLabelString: found matches '%v' in --nodes flag", rangeMatches)
				}
				var low, high int
				low, err = strconv.Atoi(rangeMatches[1])
				high, err = strconv.Atoi(rangeMatches[2])
				if low > high {
					log.Printf("WARN: buildInstanceLabelString: found node range '%v' with min %v greater than max %v; swapping min and max and proceeding anyway", v, low, high)
					low, high = high, low
				}
				if *debugLogging {
					log.Printf("buildInstanceLabelString: parsed lower bound '%v' and upper bound '%v' in node range '%v'", low, high, v)
				}
				for i := low; i <= high; i++ {
					if *debugLogging {
						log.Printf("buildInstanceLabelString: adding node n%v to the node list", i)
					}
					nodes = append(nodes, strconv.Itoa(i))
				}
			} else {
				// The field is neither a node number nor a valid range, which is probably some kind of typo
				// nb: nodes are indexed from 1, therefore ranges are not permitted to start or end at 0
				return "", fmt.Errorf("unknown node specifier '%v' in --nodes flag", v)
			}
		}
		log.Printf("buildInstanceLabelString: using node list %v", nodes)
		/*
			As above, the extra | before the list of instance names is required so we capture certain metrics like "up"
			that can't be relabeled and therefore do not have an exported_instance label. This may result in collecting
			duplicates for these metrics but dupes are the lesser of two evils in this case since the "up" metric
			is extremely useful for troubleshooting certain classes of issues.
		*/
		instanceLabelString = fmt.Sprintf("exported_instance=~\"(?:|%v-n(?:%v))\"", *nodePrefix, strings.Join(nodes, "|"))
	}
	return instanceLabelString, nil
}

func main() {
	flag.Parse()

	verString := fmt.Sprintf("promdump version %v from commit %v built %v\n", AppVersion, CommitHash, BuildTime)

	if *version {
		fmt.Printf(verString)
		os.Exit(0)
	}

	log.Printf(verString)

	if flag.NArg() > 0 {
		log.Fatalf("Too many arguments: %v. Check for typos.", strings.Join(flag.Args(), " "))
	}

	if *nodePrefix == "" && *metric == "" {
		log.Fatalln("Specify a --node_prefix value (if collecting default Yugabyte metrics), a custom metric using --metric, or both.")
	}

	if *nodePrefix != "" && *prefixValidation {
		prefixHasNodeNum, _ := regexp.Match("-n[0-9]+$", []byte(*nodePrefix))
		// If a node prefix is specified, it must begin with yb- and must not end with the node number
		// TODO: Run all the validations, then print the entire list of failures instead of throwing one-off fatals.
		if !strings.HasPrefix(*nodePrefix, "yb-") {
			log.Fatalf("Invalid --node_prefix value '%v'. Node prefix must start with 'yb-', e.g. yb-prod-my-universe.", *nodePrefix)
		}
		// TODO: Add validation for environment, e.g. yb-dev, yb-prod, etc.
		if prefixHasNodeNum {
			log.Fatalf("Invalid --node_prefix value '%v'. Node prefix must not include a node number. To filter by node, use --nodes or --instances.", *nodePrefix)
		}
	}

	if *nodePrefix == "" && *metric != "" {
		// If the user has not provided a node prefix but has provided a metric, flip default yb metrics
		// collection off.
		for _, v := range collectMetrics {
			v := v
			if !v.changedFromDefault {
				v.collect = false
			}
			if *nodePrefix == "" && *instanceList == "" && v.collect && v.requiresNodePrefix {
				log.Fatalln("Specify a --node_prefix value or a --instances value, or remove any Yugabyte metric export collection flags (--master, --node, etc.)")
			}
		}
	}

	if *metric != "" && *out == "" {
		log.Fatalln("When specifying a custom --metric, output file prefix --out is required.")
	}

	if *instanceList != "" && *nodeSet != "" {
		log.Fatalln("The --instances and --nodes flags are mutually exclusive.")
	}

	var instanceLabelString string
	var err error
	instanceLabelString, err = buildInstanceLabelString(*instanceList, *nodeSet)
	if err != nil {
		if *instanceList != "" {
			log.Fatalf("main: unable to build PromQL instance label: %v; verify that the --instances flag is correctly formatted", err)
		} else if *nodeSet != "" {
			log.Fatalf("main: unable to build PromQL instance label: %v; verify that the --nodes flag is correctly formatted", err)
		}
	}
	if instanceLabelString == "" {
		log.Println("main: not filtering by exported_instance")
	} else {
		log.Printf("main: using exported_instance filter '%v'", instanceLabelString)
		// Toggle collection of platform metrics off by default if using an exported_instance filter
		if !collectMetrics["platform"].changedFromDefault {
			log.Printf("WARN: main: metrics collection has been filtered to specific nodes; disabling collection of platform metrics (specify --platform to re-enable)")
			collectMetrics["platform"].collect = false
		}
	}

	logMetricCollectorConfig()

	if *startTime != "" && *endTime != "" && *periodDur != 0 {
		log.Fatalln("main: too many time arguments; specify either --start_time and --end_time or a time and --period")
	}

	var beginTS, endTS time.Time
	// var err error - already declared above
	beginTS, endTS, *periodDur, err = getRangeTimestamps(*startTime, *endTime, *periodDur)
	if err != nil {
		log.Fatalln("main: ", err)
	}

	// This check has moved below timestamp calculations because the period may now be a calculated value
	if periodDur.Nanoseconds()%1e9 != 0 || batchDur.Nanoseconds()%1e9 != 0 {
		log.Fatalln("main: --period and --batch must not have fractional seconds")
	}
	if *batchDur > *periodDur {
		batchDur = periodDur
	}

	ctx := context.Background()
	client, err := api.NewClient(api.Config{Address: *baseURL})
	if err != nil {
		log.Fatalln("api.NewClient: ", err)
	}
	promApi := v1.NewAPI(client)

	checkPrefixes := make([]string, 0, len(collectMetrics))
	conflictPrefixes := make([]string, 0, len(collectMetrics))

	if *out != "" {
		checkPrefixes = append(checkPrefixes, *out)
	}
	if *nodePrefix != "" {
		for _, v := range collectMetrics {
			metricName, err := getMetricName(v)
			if err != nil {
				log.Fatalf("main: %v", err)
			}
			checkPrefixes = append(checkPrefixes, metricName)
		}
	}

	for _, prefix := range checkPrefixes {
		conflict, err := hasConflictingFiles(prefix)
		if err != nil {
			log.Fatalf("main: checking for existing export files with file prefix %v failed with error: %v", prefix, err)
		}
		if conflict {
			conflictPrefixes = append(conflictPrefixes, prefix+".*")
		}
	}
	if len(conflictPrefixes) > 0 {
		sort.Strings(conflictPrefixes)
		log.Fatalf("main: found existing export files with file prefix(es): %v; move any existing export files aside before proceeding", strings.Join(conflictPrefixes, " "))
	}

	// TODO: DRY this out
	// Loop through yb metrics list and export each metric according to its configuration
	for _, v := range collectMetrics {
		if v.collect {
			metricName, err := getMetricName(v)
			if err != nil {
				log.Fatalf("main: %v", err)
			}

			var ybMetric string

			// Make an empty slice with backing array capacity of 3 to hold PromQL labels
			labels := make([]string, 0, 3)
			if v.exportName != "" {
				// Non-empty exportName implies this is a *_export query, e.g. node_export
				// TODO: Make the Prometheus labels a map?
				labels = append(labels, fmt.Sprintf("export_type=\"%s\"", metricName))
				if *nodePrefix != "" {
					// Only print the node prefix selector if a node prefix was specified. The command line args are
					// validated at an earlier stage, so if we've reached this point, it means we already know the
					// prefix isn't required.
					labels = append(labels, fmt.Sprintf("node_prefix=\"%s\"", *nodePrefix))
				}
				if instanceLabelString != "" {
					labels = append(labels, instanceLabelString)
				}
			} else if v.jobName != "" {
				// Non-empty jobName implies this is prometheus or platform or one of the other job="?" queries
				labels = append(labels, fmt.Sprintf("job=\"%s\"", metricName))
				// The prometheus and platform jobs do not have exported_instance, so no need to add
				// the corresponding label
			}
			// ['export_type="node_export"', 'node_prefix="yb-dev-..."'] => '{export_type="node_export",node_prefix="yb-dev-..."}'
			ybMetric = fmt.Sprintf("{%v}", strings.Join(labels, ","))

			v.fileCount, err = exportMetric(ctx, promApi, ybMetric, beginTS, endTS, *periodDur, *batchDur, *batchesPerFile, metricName)

			if err != nil {
				log.Printf("exportMetric: export of metric %v failed with error %v; moving to next metric", metricName, err)
				continue
			}
		}
	}
	if *metric != "" {
		customMetricCount, err = exportMetric(ctx, promApi, *metric, beginTS, endTS, *periodDur, *batchDur, *batchesPerFile, *out)
		if err != nil {
			log.Fatalln("exportMetric:", err)
		}
	}

	if *enableTar {
		if *tarFilename == "" {
			// If filename is not provided, generate a default tar filename
			*tarFilename = generateDefaultTarFilename()
		} else {
			// Check if filename already exists
			if _, err := os.Stat(*tarFilename); err == nil {
				log.Fatalf("specified --tar_filename '%s' already exists; please choose a different filename", *tarFilename)
				return
			}
		}

		TarFileOut, err := os.Create(*tarFilename)
		if err != nil {
			log.Printf("Error writing archive: %v (File: %v)\n", err, *tarFilename)
			return
		}

		// Create the archive
		err = createArchive(TarFileOut)
		if err != nil {
			log.Printf("Error creating archive: %v\n", err)
		}
		// cleaning files
		for _, v := range collectMetrics {
			v := v
			metricName, err := getMetricName(v)
			if err != nil {
				log.Printf("could not retrieve metric name for metric v: %v", err)
			}
			if v.collect {
				_, err := cleanFiles(metricName, v.fileCount)
				if err != nil {
					log.Printf("Error cleaning files : %v", err)
				}
			}
		}

		fmt.Println("Finished creating metrics bundle:", *tarFilename)

	}
}
