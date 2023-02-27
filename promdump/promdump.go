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
	"regexp"
	"time"

	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

var (
	baseURL        = flag.String("url", "http://localhost:9090", "URL for Prometheus server API")
	endTime        = flag.String("timestamp", "", "timestamp to end querying at (RFC3339). Defaults to current time.")
	periodDur      = flag.Duration("period", 7*24*time.Hour, "time period to get data for (ending at --timestamp)")
	batchDur       = flag.Duration("batch", 24*time.Hour, "batch size: time period for each query to Prometheus server.")
	metric         = flag.String("metric", "", "metric to fetch (can include label values)")
	out            = flag.String("out", "", "output file prefix")
	batchesPerFile = flag.Uint("batches_per_file", 1, "batches per output file")
)

// dump a slice of SampleStream messages to a json file.
func writeFile(values *[]*model.SampleStream, fileNum uint) error {
	/*
	   This check is duplicated here because we call writeFile unconditionally to flush any pending writes before
	   exiting and we don't want to print a stray "Writing 0 results to file" line.
	*/
	if len(*values) == 0 {
		return nil
	}
	filename := fmt.Sprintf("%s.%05d", *out, fileNum)
	valuesJSON, err := json.Marshal(values)
	if err != nil {
		return err
	}
	log.Printf("Writing %v results to file %v", len(*values), filename)
	return ioutil.WriteFile(filename, valuesJSON, 0644)
}

func cleanFiles(fileNum uint) (uint, error) {
	for i := uint(0); i < fileNum; i++ {
		filename := fmt.Sprintf("%s.%05d", *out, i)
		//log.Printf("Removing stale output file %v", filename)
		err := os.Remove(filename)
		if err != nil {
			// If removal of the first file fails, we have removed 0 files.
			log.Printf("%v stale output file(s) removed. Removal of file %v failed.", i, filename)
			return i, err
		}
	}
	if fileNum > 0 {
		log.Printf("%v stale output file(s) removed.", fileNum)
	}
	return fileNum, nil
}

func main() {
	flag.Parse()

	if *metric == "" || *out == "" {
		log.Fatalln("Please specify --metric and --out")
	}

	if periodDur.Nanoseconds()%1e9 != 0 || batchDur.Nanoseconds()%1e9 != 0 {
		log.Fatalln("--period and --batch must not have fractional seconds")
	}
	if *batchDur > *periodDur {
		batchDur = periodDur
	}

	endTS := time.Now()
	if *endTime != "" {
		var err error
		endTS, err = time.Parse(time.RFC3339, *endTime)
		if err != nil {
			log.Fatal(err)
		}
	}

	beginTS := endTS.Add(-*periodDur)
	batches := uint(math.Ceil(periodDur.Seconds() / batchDur.Seconds()))

	log.Printf("Will query from %v to %v in %v batches\n", beginTS, endTS, batches)

	ctx := context.Background()
	client, err := api.NewClient(api.Config{Address: *baseURL})
	if err != nil {
		log.Fatal(err)
	}
	promApi := v1.NewAPI(client)

	allBatchesFetched := false

	values := make([]*model.SampleStream, 0, 0)
	fileNum := uint(0)
	// There's no way to restart a loop iteration in golang, so apply brute force and ignorance to batch size backoff
	for allBatchesFetched == false {
		batches = uint(math.Ceil(periodDur.Seconds() / batchDur.Seconds()))
		if batches > 99999 {
			log.Fatal(fmt.Sprintf("batch settings could generate %v batches, which is an unreasonable number of batches", batches))
		}
		for batch := uint(1); batch <= batches; batch++ {
			queryTS := beginTS.Add(*batchDur * time.Duration(batch))
			lookback := batchDur.Seconds()
			if queryTS.After(endTS) {
				lookback -= queryTS.Sub(endTS).Seconds()
				queryTS = endTS
			}

			query := fmt.Sprintf("%s[%ds]", *metric, int64(lookback))
			log.Printf("Querying %s at %v", query, queryTS)
			// TODO: Add support for overriding the timeout; remember it can only go *smaller*
			value, _, err := promApi.Query(ctx, query, queryTS)

			if err != nil {
				// This is horrible but the golang prometheus_client swallows the 422 HTTP return code, so we have to
				// scrape instead :(
				tooManySamples, _ := regexp.Match("query processing would load too many samples into memory", []byte(err.Error()))
				if tooManySamples {
					newBatchDur := time.Duration(batchDur.Nanoseconds() / 2)
					log.Printf("Too many samples in result set. Reducing batch size from %v to %v and trying again.", batchDur, newBatchDur)
					_, err := cleanFiles(fileNum)
					fileNum = 0
					if err != nil {
						log.Fatal(fmt.Sprintf("failed to clean up stale export files: %s", err))
					}
					*batchDur = newBatchDur
					if batchDur.Seconds() <= 1 {
						log.Fatal(errors.New("failed to query Prometheus - too much data even at minimum batch size"))
					}
					break
				} else {
					log.Fatal(err)
				}
			}

			if value == nil {
				log.Fatal("did not return results")
			}

			if value.Type() != model.ValMatrix {
				log.Fatalf("Expected matrix value type; got %v", value.Type())
			}
			// model/value.go says: type Matrix []*SampleStream
			values = append(values, value.(model.Matrix)...)

			if batch%*batchesPerFile == 0 {
				if len(values) > 0 {
					err = writeFile(&values, fileNum)
					if err != nil {
						log.Fatalf("batch write failed with: %v", err)
					}
					fileNum++
					values = make([]*model.SampleStream, 0, 0)
				} else {
					log.Println("No results for this query, skipping write.")
				}
			}
			// If this is the last batch, exit the outer loop
			if batch == batches {
				allBatchesFetched = true
			}
		}
	}
	err = writeFile(&values, fileNum)
	if err != nil {
		log.Fatalf("batch write failed with: %v", err)
	}
}
