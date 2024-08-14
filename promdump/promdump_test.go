package main

import (
	"io"
	"log"
	"testing"
	"time"
)

func setupLogging() {
	// Need this to avoid crashing when a func under test tries to log
	//logger = log.Default()
	logger = log.New(io.Discard, "", 0)
}

func freezeTime() {
	// The promdump code uses a variable called now instead of using time.Now directly so that we can replace calls to
	// now() with a custom func for testing purposes.
	staticNow := time.Now().Round(time.Second).UTC()
	log.Printf("freezeTime: overriding now() to always return %v for testing purposes", staticNow.Format(time.RFC3339))
	now = func() time.Time {
		return staticNow
	}
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

type testTime struct {
	startTime string
	endTime   string
	period    time.Duration
}

func TestGetRangeTimestamps(t *testing.T) {
	setupLogging()
	freezeTime()

	testTimes := map[string]*testTime{
		"noTime":          {},
		"periodButNoTime": {period: defaultPeriod},
		"futureEndTime":   {endTime: now().Add(10 * time.Minute).Format(time.RFC3339)},
		"futureTime":      {startTime: "2037-01-01T00:00:00Z", endTime: "2037-01-01T01:23:45Z"},
		"reversedTime":    {startTime: "2024-07-16T18:00:00Z", endTime: "2024-07-15T14:30:00Z"},
		"startEndOnly":    {startTime: "2024-07-14T14:30:00Z", endTime: "2024-07-14T18:00:00Z"},
		"startPeriodOnly": {startTime: "2024-07-14T11:50:00Z", period: 15 * time.Minute},
		"endPeriodOnly":   {endTime: "2024-07-14T18:00:00Z", period: 4 * time.Hour},
		"tooManyTimes":    {startTime: "2024-07-15T12:34:56Z", endTime: "2024-07-15T12:45:36Z", period: 3 * 24 * time.Hour},
	}

	// at most two of startTime, endTime, and period can be specified
	ts := testTimes["tooManyTimes"]
	_, _, _, err := getRangeTimestamps(ts.startTime, ts.endTime, ts.period)
	if err == nil {
		t.Errorf("getRangeTimestamps(%v, %v, %v) must return an error when all of startTime, endTime, and period are specified", ts.startTime, ts.endTime, ts.period)
	}

	// endTime should default to now if neither the start time nor end time are specified
	ts = testTimes["noTime"]
	_, testTs, _, err := getRangeTimestamps(ts.startTime, ts.endTime, ts.period)
	if testTs != now() {
		t.Errorf("getRangeTimestamps(%v, %v, %v) should return an end timestamp of now if no start time or end time are specified; expected %v got %v", ts.startTime, ts.endTime, ts.period, now(), testTs)
	}

	// endTime should default to now if only the period is specified
	ts = testTimes["periodButNoTime"]
	_, testTs, _, err = getRangeTimestamps(ts.startTime, ts.endTime, ts.period)
	if testTs != now() {
		t.Errorf("getRangeTimestamps(%v, %v, %v) should return an end timestamp of now if no start time or end time are specified; expected %v got %v", ts.startTime, ts.endTime, ts.period, now(), testTs)
	}

	// TODO: Add tests for parsing startTime and endTime? Might be challenging to implement in a way that does a meaningful test

	// When both start time and end time are specified, the period is the difference between the two
	ts = testTimes["startEndOnly"]
	expectedPeriod := 3*time.Hour + 30*time.Minute
	_, _, period, _ := getRangeTimestamps(ts.startTime, ts.endTime, ts.period)
	if period != expectedPeriod {
		t.Errorf("getRangeTimestamps(%v, %v, %v) should return a period that is the difference between end time %v and start time %v; expected %v got %v", ts.startTime, ts.endTime, ts.period, ts.endTime, ts.startTime, expectedPeriod, period)
	}

	// When none of start time, end time, or period are specified, the period should be the default period
	ts = testTimes["noTime"]
	_, _, period, _ = getRangeTimestamps(ts.startTime, ts.endTime, ts.period)
	if period != defaultPeriod {
		t.Errorf("getRangeTimestamps(%v, %v, %v) should return the default period when no parameters are specified; expected %v got %v", ts.startTime, ts.endTime, ts.period, defaultPeriod, period)
	}

	// When we have the period and one time stamp, test whether the other time stamp is calculated correctly
	// One test for calculating end time
	ts = testTimes["startPeriodOnly"] // 15 minutes after 2024-07-14T11:50:00Z should be 2024-07-14T12:05:00Z
	_, testTs, _, err = getRangeTimestamps(ts.startTime, ts.endTime, ts.period)
	if err != nil {
		t.Errorf("getRangeTimestamps(%v, %v, %v) unexpectedly returned an error %v", ts.startTime, ts.endTime, ts.period, err)
	}
	rightTs, _ := time.Parse(time.RFC3339, "2024-07-14T12:05:00Z")
	if testTs != rightTs {
		t.Errorf("getRangeTimestamps(%v, %v, %v) should return an end timestamp that is the start time plus the period; expected %v got %v", ts.startTime, ts.endTime, ts.period, rightTs, testTs)
	}

	// One test for calculating start time
	ts = testTimes["endPeriodOnly"] // 4 hours before 2024-07-14T18:00:00Z should be 2024-07-14T14:00:00Z
	testTs, _, _, _ = getRangeTimestamps(ts.startTime, ts.endTime, ts.period)
	if err != nil {
		t.Errorf("getRangeTimestamps(%v, %v, %v) unexpectedly returned an error %v", ts.startTime, ts.endTime, ts.period, err)
	}
	rightTs, _ = time.Parse(time.RFC3339, "2024-07-14T14:00:00Z")
	if testTs != rightTs {
		t.Errorf("getRangeTimestamps(%v, %v, %v) should return a start timestamp that is the end time minus the period; expected %v got %v", ts.startTime, ts.endTime, ts.period, rightTs, testTs)
	}

	ts = testTimes["futureTime"]
	// startTime must be in the past
	// This will break in 2037 but I desperately hope promdump is not still in use by then
	_, _, _, err = getRangeTimestamps(ts.startTime, ts.endTime, ts.period)
	if err == nil {
		t.Errorf("getRangeTimestamps(%v, %v, %v) must return an error when startTime is in the future; expected %v < %v", ts.startTime, ts.endTime, ts.period, ts.startTime, now().Format(time.RFC3339))
	}

	ts = testTimes["reversedTime"]
	// startTime must be before endTime
	_, _, _, err = getRangeTimestamps(ts.startTime, ts.endTime, ts.period)
	if err == nil {
		t.Errorf("getRangeTimestamps(%v, %v, %v) must return an error when startTime is after endTime; expected %v < %v", ts.startTime, ts.endTime, ts.period, ts.startTime, ts.endTime)
	}

	// TODO: This test is entangled with the period calculation tests startEndOnly and noTime. Disentangle these.
	// end timestamp should be set to now if it's a time from the future
	// set the end time to now (using the testing version of now()) + 10 minutes
	beyondJourneysEndTime := testTime{endTime: now().Add(10 * time.Minute).Format(time.RFC3339)}
	_, testTs, _, err = getRangeTimestamps(beyondJourneysEndTime.startTime, beyondJourneysEndTime.endTime, beyondJourneysEndTime.period)
	if !testTs.Equal(now()) {
		t.Errorf("getRangeTimestamps(%v, %v, %v) should return an end timestamp of now if the end time is from the future; expected %v got %v", beyondJourneysEndTime.startTime, beyondJourneysEndTime.endTime, beyondJourneysEndTime.period, now(), testTs)
	}
}
