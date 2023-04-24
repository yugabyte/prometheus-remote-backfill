// promremotewrite reads SampleStream messages from a series of json files and
// sends those to a service that supports Prometheus remote write endpoint
// (remote_storage_adapter, influxdb, etc).
// json files can be generated by promdump.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"golang.org/x/net/context/ctxhttp"
)

var (
	version      = flag.Bool("version", false, "prints the promdump version and exits")
	writeURL     = flag.String("url", "", "URL for remote write endpoint")
	writeTimeout = flag.Duration("write_timeout", 5*time.Minute, "write timeout")
	batchSize    = flag.Uint("batch_size", 100000, "number of samples per request")
	concurrency  = flag.Uint("concurrency", 1, "number of influxdb writers")

	AppVersion = "DEV BUILD"
	CommitHash = "POPULATED_BY_BUILD"
	BuildTime  = "POPULATED_BY_BUILD"
)

// converts a slice of SampleStream messages into remote write requests and sends them into the channel.
func generateWriteRequests(streams []*model.SampleStream, requests chan<- *prompb.WriteRequest) {
	var req = &prompb.WriteRequest{
		Timeseries: make([]prompb.TimeSeries, 0, 0),
	}
	totalSamples := uint(0)
	for _, s := range streams {
		samples := make([]prompb.Sample, 0, len(s.Values))
		for _, v := range s.Values {
			samples = append(samples, prompb.Sample{
				Value:     float64(v.Value),
				Timestamp: int64(v.Timestamp),
			})
			totalSamples++
		}
		ts := prompb.TimeSeries{
			Labels:  metricToLabelProtos(s.Metric),
			Samples: samples,
		}
		req.Timeseries = append(req.Timeseries, ts)
		if totalSamples > *batchSize {
			log.Printf("Sending batch of %d samples", totalSamples)
			totalSamples = 0
			requests <- req
			req = &prompb.WriteRequest{Timeseries: make([]prompb.TimeSeries, 0, 0)}
		}
	}

	log.Printf("Sending batch of %d samples", totalSamples)
	requests <- req
}

// metricToLabelProtos builds a []*prompb.Label from a model.Metric
// Copy/pasted from prometheus/storage/remote/codec.go (can't use it directly
// because of vendoring in prometheus repo, see prometheus/issues/1720).
func metricToLabelProtos(metric model.Metric) []prompb.Label {
	labels := make([]prompb.Label, 0, len(metric))
	for k, v := range metric {
		labels = append(labels, prompb.Label{
			Name:  string(k),
			Value: string(v),
		})
	}
	sort.Slice(labels, func(i int, j int) bool {
		return labels[i].Name < labels[j].Name
	})
	return labels
}

// write sends a WriteRequest to a remote write endpoint using an http client.
// Copy/pasted from prometheus/storage/remote/client.go.
func write(client *http.Client, req *prompb.WriteRequest) error {
	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	compressed := snappy.Encode(nil, data)
	httpReq, err := http.NewRequest("POST", *writeURL, bytes.NewReader(compressed))
	if err != nil {
		// Errors from NewRequest are from unparseable URLs, so are not
		// recoverable.
		return err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	ctx, cancel := context.WithTimeout(context.Background(), *writeTimeout)
	defer cancel()

	httpResp, err := ctxhttp.Do(ctx, client, httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, 2048))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		err = fmt.Errorf("server returned HTTP status %s: %s", httpResp.Status, line)
	}
	if httpResp.StatusCode/100 == 5 {
		return err
	}
	return err
}

func main() {
	flag.Parse()

	verString := fmt.Sprintf("promremotewrite version %v from commit %v built %v\n", AppVersion, CommitHash, BuildTime)

	if *version {
		fmt.Printf(verString)
		os.Exit(0)
	}

	log.Printf(verString)

	if *writeURL == "" {
		log.Fatalln("Please specify --url")
	}

	if len(flag.Args()) == 0 {
		log.Fatalln("Please specify at least one input file as a command line argument")
	}

	// Buffer 20 requests in RAM to allow the next json file to be read while
	// we still send requests for the previous one.
	requests := make(chan *prompb.WriteRequest, 20)
	var wg sync.WaitGroup
	for i := uint(0); i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := &http.Client{}
			for r := range requests {
				if err := write(c, r); err != nil {
					log.Fatal(err)
				}
			}
		}()
	}

	for _, fname := range flag.Args() {
		log.Printf("Processing file %s", fname)
		contents, err := os.ReadFile(fname)
		if err != nil {
			log.Fatal(err)
		}

		var values []*model.SampleStream
		err = json.Unmarshal(contents, &values)
		if err != nil {
			log.Fatal(err)
		}
		generateWriteRequests(values, requests)
	}
	close(requests)
	wg.Wait()
}
