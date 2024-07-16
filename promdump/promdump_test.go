package main

import (
	"io"
	"log"
	"testing"
)

func setupLogging() {
	// Need this to avoid crashing when a func under test tries to log
	//logger = log.Default()
	logger = log.New(io.Discard, "", 0)
}

func TestGetMetricName(t *testing.T) {
	setupLogging()

	testMetrics := map[string]*promExport{
		"emptymetric": {collect: true, changedFromDefault: false, requiresNodePrefix: false},
		"exporter":    {exportName: "exporter_export", collect: true, changedFromDefault: false, requiresNodePrefix: true},
		"jobmetric":   {jobName: "jobmetric", collect: true, changedFromDefault: false, requiresNodePrefix: false},
		"badmetric":   {exportName: "badmetric_export", jobName: "badmetric", collect: true, changedFromDefault: false, requiresNodePrefix: true},
	}

	testMetric := testMetrics["emptymetric"]
	// presence of one of exportName or jobName must be enforced
	metricName, err := getMetricName(testMetric)
	if err == nil {
		t.Errorf("getMetricName(%+v) must return an error when neither exportName and jobName are present", testMetric)
	}

	testMetric = testMetrics["badmetric"]
	// mutual exclusivity of exportName and jobName must be enforced
	metricName, err = getMetricName(testMetric)
	if err == nil {
		t.Errorf("getMetricName(%+v) must return an error when both exportName and jobName are present", testMetric)
	}

	testMetric = testMetrics["exporter"]
	// getMetricName must return the export name for exporter metrics
	metricName, err = getMetricName(testMetric)
	if metricName != "exporter_export" {
		t.Errorf("getMetricName(%+v) must return the name of the export", testMetric)
	}

	testMetric = testMetrics["jobmetric"]
	// getMetricName must return the job name for job metrics
	metricName, err = getMetricName(testMetric)
	if metricName != "jobmetric" {
		t.Errorf("getMetricName(%+v) must return the name of the job", testMetric)
	}
}
