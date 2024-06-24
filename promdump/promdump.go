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
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	ywclient "github.com/yugabyte/platform-go-client"
	"io"
	"log"
	"math"
	"net"
	"net/http"
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
	"github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

const defaultPeriod = 24 * time.Hour
const defaultBatchDuration = 15 * time.Minute
const defaultYbaHostname = "localhost"
const defaultPromPort = 9090

type promExport struct {
	exportName         string
	jobName            string
	collect            bool
	changedFromDefault bool
	requiresNodePrefix bool
	fileCount          uint
}

var (
	defaultBaseUrl = fmt.Sprintf("http://%v:%v", defaultYbaHostname, defaultPromPort)

	// Also see init() below for aliases
	debugLogging             = flag.Bool("debug", false, "enable additional debug logging")
	version                  = flag.Bool("version", false, "prints the promdump version and exits")
	listUniverses            = flag.Bool("list_universes", false, "prints the list of Universes known to YBA and exits; requires a --yba_api_token")
	baseURL                  = flag.String("url", defaultBaseUrl, "URL for Prometheus server API")
	skipPromHostVerification = flag.Bool("skip_prometheus_host_verification", false, "bypasses TLS certificate verification for Prometheus queries (insecure)")
	promApiTimeout           = flag.Duration("prometheus_api_timeout", 10, "the HTTP timeout to use for Prometheus API calls, in seconds (optional)")
	startTime                = flag.String("start_time", "", "RFC3339 `timestamp` to start querying at (e.g. 2023-03-13T01:00:00-0100).")
	endTime                  = flag.String("end_time", "", "RFC3339 `timestamp` to end querying at (default now)")
	periodDur                = flag.Duration("period", 0, "time period to get data for")
	batchDur                 = flag.Duration("batch", defaultBatchDuration, "batch size: time period for each query to Prometheus server.")
	metric                   = flag.String("metric", "", "custom metric to fetch (optional; can include label values)")
	out                      = flag.String("out", "", "output file prefix; only used for custom --metric specifications")
	nodePrefix               = flag.String("node_prefix", "", "node prefix value for Yugabyte Universe, e.g. yb-prod-appname (deprecated)")
	prefixValidation         = flag.Bool("node_prefix_validation", true, "set to false to disable node prefix validation")
	universeName             = flag.String("universe_name", "", "the name of the Universe for which to collect metrics, as shown in the YBA UI")
	universeUuid             = flag.String("universe_uuid", "", "the UUID of the Universe for which to collect metrics")
	instanceList             = flag.String("instances", "", "the instance name(s) for which to collect metrics (optional, mutually exclusive with --nodes; comma separated list, e.g. yb-prod-appname-n1,yb-prod-appname-n3,yb-prod-appname-n4,yb-prod-appname-n5,yb-prod-appname-n6,yb-prod-appname-n14; disables collection of platform metrics unless explicitly enabled with --platform")
	nodeSet                  = flag.String("nodes", "", "the node number(s) for which to collect metrics (optional, mutually exclusive with --instances); comma separated list of node numbers or ranges, e.g. 1,3-6,14; disables collection of platform metrics unless explicitly requested with --platform")
	batchesPerFile           = flag.Uint("batches_per_file", 1, "batches per output file")
	enableTar                = flag.Bool("tar", true, "enable bundling exported metrics into a tar file")
	tarCompression           = flag.String("tar_compression_algorithm", "gzip", "compression algorithm to use when creating a tar bundle; one of \"gzip\", \"bzip2\", or \"none\"")
	tarFilename              = flag.String("tar_filename", "", "filename for the generated tar file")
	keepFiles                = flag.Bool("keep_files", false, "preserve metric export files after archiving them")
	useYbaApi                = false
	ybaHostname              = flag.String("yba_api_hostname", defaultYbaHostname, "the hostname to use for calls to the YBA API (optional)")
	ybaApiTimeout            = flag.Duration("yba_api_timeout", 10, "the HTTP timeout to use for YBA API calls, in seconds (optional)")
	ybaToken                 = flag.String("yba_api_token", "", "the API token to use for communication with YBA (optional)")
	ybaTls                   = flag.Bool("yba_api_use_tls", true, "set to false to disable TLS for YBA API calls (insecure)")
	skipYbaHostVerification  = flag.Bool("skip_yba_host_verification", false, "bypasses TLS certificate verification for YBA API calls (insecure)")

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
	customMetricCount uint // Holds custom metric file counts

	AppVersion = "DEV BUILD"
	CommitHash = "POPULATED_BY_BUILD"
	BuildTime  = "POPULATED_BY_BUILD"
)

func init() {
	flag.BoolVar(version, "v", false, "prints the promdump version and exits")
	flag.StringVar(endTime, "timestamp", "", "alias for end_time (`timestamp`)")
	flag.StringVar(ybaHostname, "yba_hostname", defaultYbaHostname, "alias for yba_api_hostname")
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

func cleanFiles(filePrefix string, fileNum uint, verbose bool) (uint, error) {
	for i := uint(0); i < fileNum; i++ {
		filename := fmt.Sprintf("%s.%05d", filePrefix, i)
		err := os.Remove(filename)
		if err != nil {
			// If removal of the first file fails, we have removed 0 files.
			log.Printf("cleanFiles: %v stale output file(s) removed. Removal of file %v failed.", i, filename)
			return i, err
		}
	}
	if fileNum > 0 && verbose {
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
					_, err := cleanFiles(filePrefix, fileNum, true)
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
		log.Fatalf("unsupported compression type: %v ", *tarCompression)
		return nil

	}
	defer tw.Close()
	log.Printf("createArchive: using tar compression format '%s'", *tarCompression)

	// Iterate over collectMetrics and add them to the tar archive
	for _, v := range collectMetrics {
		metricName, err := getMetricName(v)
		for i := uint(0); i < v.fileCount; i++ {
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
		for i := uint(0); i < customMetricCount; i++ {
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
	switch *tarCompression {
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

func setupPromAPI(ctx context.Context) (v1.API, error) {
	tlsCc := &tls.Config{
		InsecureSkipVerify: *skipPromHostVerification,
	}

	tr := &http.Transport{
		TLSClientConfig: tlsCc,
	}

	httpClient := &http.Client{
		Timeout:   time.Second * *promApiTimeout,
		Transport: tr,
	}

	apiClient, err := api.NewClient(api.Config{Address: *baseURL, Client: httpClient})
	if err != nil {
		return nil, fmt.Errorf("failed to create Prometheus API client: %w", err)
	}
	return v1.NewAPI(apiClient), nil
}

func setupYBAAPI(ctx context.Context) (*ywclient.APIClient, error) {
	// A very large number of customers are using self-signed certificates, so we need to be able to turn off
	// certificate verification.
	tlsCc := &tls.Config{
		InsecureSkipVerify: *skipYbaHostVerification,
	}

	tr := &http.Transport{
		TLSClientConfig: tlsCc,
	}

	httpClient := &http.Client{
		Timeout:   time.Second * *ybaApiTimeout,
		Transport: tr,
	}

	configuration := ywclient.NewConfiguration()
	if *ybaTls {
		configuration.Scheme = "https"
	} else {
		configuration.Scheme = "http"
	}

	log.Printf("Using hostname '%v' to connect to the YBA API", *ybaHostname)

	// Validate the provided YBA hostname by performing a hostname lookup on it. The behaviour of this lookup may
	// vary by operating system. Discard the returned IP address list because we don't actually care what the IP is,
	// only that hostname resolution succeeded.
	_, err := net.LookupHost(*ybaHostname)
	if err != nil {
		log.Fatalf("YBA hostname lookup failed: %v", err)
	}
	configuration.Host = *ybaHostname
	configuration.Debug = *debugLogging
	configuration.HTTPClient = httpClient

	return ywclient.NewAPIClient(configuration), nil
}

func getCustomerUuid(ctx context.Context, client *ywclient.APIClient) (string, error) {
	if *debugLogging {
		log.Println("Making YBA API call ListOfCustomers")
	}
	customers, r, err := client.CustomerManagementApi.ListOfCustomers(ctx).Execute()
	if err != nil {
		return "", fmt.Errorf("API call ListOfCustomers failed: %w", err)
	}
	defer func() {
		err := r.Body.Close()
		if err != nil {
			log.Fatalf("getCustomerUuid: failed to close HTTP response body: %v", err)
		}
	}()
	// Status codes between 200 and 299 indicate success
	statusOK := r.StatusCode >= 200 && r.StatusCode < 300
	if !statusOK {
		// In case we later need special handling for specific http status codes
		var msg error
		switch r.StatusCode {
		default:
			msg = fmt.Errorf("http request failed with status %v", r.StatusCode)
		}
		return "", fmt.Errorf("API call ListOfCustomers failed: %w", msg)
	}
	if len(customers) > 1 {
		return "", fmt.Errorf("multi-tenant environments are not currently supported")
	}
	customer := customers[0]

	if *debugLogging {
		log.Printf("getCustomerUuid: found customer '%v' with UUID '%v'", customer.Name, *customer.Uuid)
	}
	return *customer.Uuid, nil
}

func getUniverseList(ctx context.Context, client *ywclient.APIClient, cUuid string) ([]ywclient.UniverseResp, error) {
	universes, r, err := client.UniverseManagementApi.ListUniverses(ctx, cUuid).Execute()
	if err != nil {
		return []ywclient.UniverseResp{}, fmt.Errorf("API call ListUniverses failed: %w", err)
	}
	defer func() {
		err := r.Body.Close()
		if err != nil {
			log.Fatalf("getUniverseList: failed to close HTTP response body: %v", err)
		}
	}()
	statusOK := r.StatusCode >= 200 && r.StatusCode < 300
	if !statusOK {
		var msg error
		switch r.StatusCode {
		default:
			msg = fmt.Errorf("http request failed with status %v", r.StatusCode)
		}
		return []ywclient.UniverseResp{}, fmt.Errorf("API call GetUniverse failed: %w", msg)
	}

	return universes, nil
}

func getUniverseByName(ctx context.Context, client *ywclient.APIClient, cUuid string, universeName string) (ywclient.UniverseResp, error) {
	// TODO: This could probably be DRY'd out
	universes, r, err := client.UniverseManagementApi.ListUniverses(ctx, cUuid).Name(universeName).Execute()
	if err != nil {
		return ywclient.UniverseResp{}, fmt.Errorf("API call ListUniverses failed: %w", err)
	}
	defer func() {
		err := r.Body.Close()
		if err != nil {
			log.Fatalf("getUniverseByName: failed to close HTTP response body: %v", err)
		}
	}()
	statusOK := r.StatusCode >= 200 && r.StatusCode < 300
	if !statusOK {
		var msg error
		switch r.StatusCode {
		default:
			msg = fmt.Errorf("http request failed with status %v", r.StatusCode)
		}
		return ywclient.UniverseResp{}, fmt.Errorf("API call GetUniverse failed: %w", msg)
	}
	// If we get back 0 Universes, it probably means we were given a bad --universe_name
	if len(universes) == 0 {
		return ywclient.UniverseResp{}, fmt.Errorf("universe with name '%v' not found", universeName)
	}
	// Universe names must be unique, so the first Universe should be the only Universe
	if len(universes) > 1 {
		return ywclient.UniverseResp{}, fmt.Errorf("multiple universes with universe name '%v' found, which should never happen", universeName)
	}
	universe := universes[0]

	return universe, nil
}

func getUniverseByUuid(ctx context.Context, client *ywclient.APIClient, cUuid string, universeUuid string) (ywclient.UniverseResp, error) {
	universe, r, err := client.UniverseManagementApi.GetUniverse(ctx, cUuid, universeUuid).Execute()
	statusOK := r.StatusCode >= 200 && r.StatusCode < 300
	// The YBA API is doing the wrong thing here. The GetUniverse call returns an error if the HTTP call succeeds but
	// the server responds with an HTTP error code. This violates the semantics of the Golang http client in various
	//  ways.
	if err != nil || !statusOK {
		switch r.StatusCode {
		case 400:
			// Response Body:
			// {
			//   "success":false,
			//   "httpMethod":"GET",
			//   "requestUri":"/api/v1/customers/ca1b9bda-0000-0000-0000-2b8068ccd5e2/universes/fdb24ab4-0000-0000-0000-6763d1af32d0",
			//   "error":"Cannot find universe fdb24ab4-0000-0000-0000-6763d1af32d0"
			// }
			var apiError ywclient.YBPError
			body, err := io.ReadAll(r.Body)
			if err != nil {
				return ywclient.UniverseResp{}, fmt.Errorf("failed to read response body while handling HTTP error: %w", err)
			}
			err = json.Unmarshal(body, &apiError)
			if err != nil {
				return ywclient.UniverseResp{}, fmt.Errorf("failed to unmarshal JSON response while handling HTTP error: %w", err)
			}
			if strings.HasPrefix(*apiError.Error, "Cannot find universe") {
				return ywclient.UniverseResp{}, fmt.Errorf("universe with UUID '%v' not found", universeUuid)
			} else {
				return ywclient.UniverseResp{}, fmt.Errorf("unknown error while handling HTTP error 400 from YBA API; bad response: %+v", r)
			}
		default:
			return ywclient.UniverseResp{}, fmt.Errorf("HTTP request failed with status %v; bad response: %+v", r.StatusCode, r)
		}
	}
	// We are deliberately leaking a socket here because there are cases that might lead to a double close, which would
	// crash the promdump utility. promdump is a short-lived process and the leaked socket will be cleaned up on
	// exit anyway.
	// defer r.Body.Close()
	log.Printf("Found Universe '%v'", *universe.Name)

	return universe, nil
}

// TODO: Should this use a writer?
func printUniverseList(universes []ywclient.UniverseResp) {
	fmt.Println()
	fmt.Printf("%4v\t%-36v\t%-30v\n", "#", "Universe UUID", "Universe Name")
	fmt.Printf("%v\t%v\t%v\n", strings.Repeat("-", 4), strings.Repeat("-", 36), strings.Repeat("-", 30))
	//log.Println("#   \tUniverse UUID                       \tUniverse Name")
	//fmt.Println("----\t------------------------------------\t------------------------------")
	for k, v := range universes {
		fmt.Printf("%4v\t%v\t%v\n", k+1, *v.UniverseUUID, *v.Name)
	}
	fmt.Println()
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

	// Don't include ybaHostname in the list of flags that trigger YBA API mode because it has a default value
	if *listUniverses || *universeName != "" || *universeUuid != "" || *ybaToken != "" {
		useYbaApi = true
	}

	if useYbaApi {
		if *ybaToken == "" {
			log.Fatalln("The --yba_api_token flag is required when using the YBA API. See the YBA API documentation at: https://api-docs.yugabyte.com/")
		}

		// The customer has specified a node prefix and also a flag that activates YBA mode. This is not allowed.
		if *nodePrefix != "" {
			log.Fatalln("The --node_prefix flag is incompatible with the YBA API. Use --universe_name or --universe_uuid instead.")
		}

		if (*universeName == "" && *universeUuid == "") && !*listUniverses {
			log.Fatalln("One of --universe_name or --universe_uuid must be specified when using the YBA API.")
		}

		if *universeName != "" && *universeUuid != "" {
			log.Fatalln("The --universe_name and --universe_uuid flags are mutually exclusive.")
		}

		if *universeUuid != "" {
			// fdb24ab4-0000-0000-0000-6763d1af32d9
			isValidUuid, err := regexp.Match("^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$", []byte(*universeUuid))
			if err != nil {
				log.Fatalf("Failed to validate --universe_uuid flag: %v", err)
			}
			if !isValidUuid {
				log.Fatalf("Invalid UUID in --universe_uuid flag. UUIDs must be hexadecimal digits and dashes in the format 'fdb24ab4-0000-0000-0000-6763d1af32d9'")
			}
		}

		if !*ybaTls {
			log.Printf("Warning: Disabling TLS for YBA communication is insecure and not recommended!")
		} else if *skipYbaHostVerification { // ybaTls is implicitly true here
			// Only reached if TLS is enabled and skipYbaHostVerification is true
			log.Println("Warning: Disabling YBA host verification is insecure and not recommended!")
		}

		log.Println("main: Connecting to YBA API")

		// Create a context with the YBA API token in it to pass into functions that make YBA API calls
		ybaCtx := context.WithValue(context.Background(),
			ywclient.ContextAPIKeys,
			map[string]ywclient.APIKey{
				"apiKeyAuth": {
					Key: *ybaToken,
				},
			})

		ybaClient, err := setupYBAAPI(ybaCtx)
		if err != nil {
			log.Fatalf("Failed to initialize the YBA API: %v", err)
		}

		cUuid, err := getCustomerUuid(ybaCtx, ybaClient)
		if err != nil {
			log.Fatalf("Failed to retrieve customer UUID from YBA: %v", err)
		}
		log.Printf("Found customer UUID '%v'", cUuid)

		if *listUniverses {
			universes, err := getUniverseList(ybaCtx, ybaClient, cUuid)
			if err != nil {
				log.Fatalf("getUniverseList: failed with: %v", err)
			}
			printUniverseList(universes)
			os.Exit(0)
		}

		var universe ywclient.UniverseResp
		if *universeName != "" {
			log.Printf("Looking up Universe with name '%v' using the YBA API", *universeName)
			universe, err = getUniverseByName(ybaCtx, ybaClient, cUuid, *universeName)
			if err != nil {
				log.Printf("getUniverseByName: failed with: %v", err)
				universes, err := getUniverseList(ybaCtx, ybaClient, cUuid)
				if err != nil {
					log.Fatalf("getUniverseList: failed with: %v", err)
				}
				printUniverseList(universes)
				log.Fatalf("Specify a Universe from the list above using --universe_uuid or --universe_name")
			}
			log.Printf("Found Universe '%v'", *universe.Name)
		} else if *universeUuid != "" {
			log.Printf("Looking up Universe with UUID '%v' using the YBA API", *universeUuid)
			universe, err = getUniverseByUuid(ybaCtx, ybaClient, cUuid, *universeUuid)
			if err != nil {
				log.Printf("getUniverseByUuid: failed with: %v", err)
				universes, err := getUniverseList(ybaCtx, ybaClient, cUuid)
				if err != nil {
					log.Fatalf("getUniverseList: failed with: %v", err)
				}
				printUniverseList(universes)
				log.Fatalf("Specify a Universe from the list above using --universe_uuid or --universe_name")
			}
		}
		log.Printf("Found node prefix '%v' for Universe '%v'", *universe.UniverseDetails.NodePrefix, *universe.Name)
		*nodePrefix = *universe.UniverseDetails.NodePrefix
		// Since we got the node prefix directly from YBA, we're going to assume it's correct and  turn validation OFF
		*prefixValidation = false

		log.Println("main: Finished with YBA API")
	} else {
		log.Printf("Warning: The --node_prefix flag is deprecated. It is recommended to provide a --yba_api_token and use --universe_name or --universe_uuid instead.")
	}

	if *nodePrefix == "" && *metric == "" {
		log.Fatalln("Specify a universe by name or UUID (if collecting default Yugabyte metrics), a custom metric using --metric, or both.")
	}

	if *nodePrefix != "" && *prefixValidation {
		prefixHasNodeNum, _ := regexp.Match("-n[0-9]+$", []byte(*nodePrefix))
		validPrefixFormat, _ := regexp.Match("^yb-(?:dev|demo|stage|preprod|prod)-[a-zA-Z0-9]([-a-zA-Z0-9]*[a-zA-Z0-9])?$", []byte(*nodePrefix))
		// The node prefix must not end with a node number. This is a common error, so we check it specifically.
		if prefixHasNodeNum {
			log.Fatalf("Invalid --node_prefix value '%v'. Node prefix must not include a node number. Use --nodes or --instances to filter by node.", *nodePrefix)
		}
		// If a node prefix is specified, it must begin with yb-, followed by the environment name and a valid
		// Universe name. Universe names are limited to alphanumeric characters, plus dash. They must begin and end
		// with an alphanumeric character.
		if !validPrefixFormat {
			log.Fatalf("Invalid --node_prefix value '%v'. Node prefixes must be in the format 'yb-<dev|demo|stage|preprod|prod>-<universe-name>', e.g. 'yb-prod-my-universe'.", *nodePrefix)
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

	if *enableTar {
		log.Printf("main: tar bundling enabled")
		if *tarFilename == "" {
			// If filename is not provided, generate a default tar filename
			*tarFilename = generateDefaultTarFilename()
			log.Printf("main: no --tar_filename specified; using filename '%s'", *tarFilename)
		}
		// Check if file with specified (or generated) filename already exists
		if _, err := os.Stat(*tarFilename); err == nil {
			log.Fatalf("specified --tar_filename '%s' already exists; please choose a different filename", *tarFilename)
		}
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

	log.Printf("main: Beginning metric collection against Prometheus endpoint '%v'", *baseURL)

	ctx := context.Background()
	promApi, err := setupPromAPI(ctx)
	if err != nil {
		log.Fatalln("setupPromAPI: ", err)
	}

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
	log.Println("main: Finished with Prometheus connection")

	if *enableTar {
		tarFileOut, err := os.Create(*tarFilename)
		if err != nil {
			log.Fatalf("Error writing archive: %v (File: %v)\n", err, *tarFilename)
		}

		// Create the archive
		log.Printf("main: creating tar archive of metric export files")
		err = createArchive(tarFileOut)
		if err != nil {
			log.Fatalf("Error creating archive: %v\n", err)
		}
		// cleaning files
		if !*keepFiles {
			log.Println("main: tar archive created successfully; cleaning metric export files")
			for _, v := range collectMetrics {
				v := v
				metricName, err := getMetricName(v)
				if err != nil {
					log.Printf("could not retrieve metric name for metric v: %v", err)
				}
				if v.collect {
					_, err := cleanFiles(metricName, v.fileCount, false)
					if err != nil {
						log.Printf("Error cleaning files : %v", err)
					}
				}
			}
		} else {
			log.Println("main: preserving metric export files because the --keep_files flag is set")
		}

		log.Printf("main: finished creating metrics bundle '%s'", *tarFilename)
	}
}
