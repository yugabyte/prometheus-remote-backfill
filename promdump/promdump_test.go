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

type testMetric struct {
	testParams     promExport
	testErr        bool
	testReturn     bool
	expectedReturn string
	errStr         string
}

func TestGetMetricName(t *testing.T) {
	setupLogging()

	testMetrics := map[string]*testMetric{
		"emptymetric": {
			testParams: promExport{collect: true, changedFromDefault: false, requiresNodePrefix: false},
			testErr:    true,
			errStr:     "must return an error when neither exportName nor jobName are present",
		},
		"exporter": {
			testParams:     promExport{exportName: "exporter_export", collect: true, changedFromDefault: false, requiresNodePrefix: true},
			testReturn:     true,
			expectedReturn: "exporter_export",
			errStr:         "must return the name of the export when specified",
		},
		"jobmetric": {
			testParams:     promExport{jobName: "jobmetric", collect: true, changedFromDefault: false, requiresNodePrefix: false},
			testReturn:     true,
			expectedReturn: "jobmetric",
			errStr:         "must return the name of the job when specified",
		},
		"badmetric": {
			testParams: promExport{exportName: "badmetric_export", jobName: "badmetric", collect: true, changedFromDefault: false, requiresNodePrefix: true},
			testErr:    true,
			errStr:     "must return an error when both exportName and jobName are present",
		},
	}

	for testName, testMetric := range testMetrics {
		if (!testMetric.testErr && !testMetric.testReturn) || (testMetric.testErr && testMetric.testReturn) {
			t.Errorf("TestGetMetricName: bad test configuration in test '%v': tests must have exactly one test condition", testName)
		}
		metricName, err := getMetricName(&testMetric.testParams)
		if testMetric.testErr && err == nil {
			t.Errorf("getMetricName(%+v) %v", testMetric.testParams, testMetric.errStr)
		}
		// If we're not testing for errors and we got an error, that's a problem
		if !testMetric.testErr && err != nil {
			t.Errorf("getMetricName(%v) unexpectedly returned an error %v", testMetric.testParams, err)
		}

		if testMetric.testReturn && metricName != testMetric.expectedReturn {
			t.Errorf("getMetricName(%+v) %v", testMetric.testParams, testMetric.errStr)
		}
	}
}

type timeTestParams struct {
	startTime string
	endTime   string
	period    time.Duration
}

type testTime struct {
	testParams timeTestParams
	testErr    bool
	testReturn bool
	// Expected values are all pointers because this simplifies the control flow. We can check whether the pointer is
	// nil and if it is, skip checking the value. If we didn't do this, we'd have to use bools or some other mechanism
	// to control the checks or instantiate empty objects for comparison.
	expectedStartTs *time.Time
	expectedEndTs   *time.Time
	expectedPeriod  *time.Duration
	errStr          string
}

func TestGetRangeTimestamps(t *testing.T) {
	setupLogging()
	freezeTime()
	// In order to use the value from the now() func in a struct such as our test structs below, we need to evaluate it first
	currentTs := now()

	testTimes := map[string]*testTime{
		"noTime": {
			testParams:    timeTestParams{},
			testReturn:    true,
			expectedEndTs: &currentTs,
			errStr:        "should return an end timestamp of now if no start time or end time are specified",
		},
		"periodButNoTime": {
			testParams:    timeTestParams{period: defaultPeriod},
			testReturn:    true,
			expectedEndTs: &currentTs,
			errStr:        "should return an end timestamp of now if no start time or end time are specified",
		},
		// TODO: Add tests for parsing startTime and endTime? Might be challenging to implement in a way that does a meaningful test
		"futureEndTime": {
			testParams:    timeTestParams{endTime: now().Add(10 * time.Minute).Format(time.RFC3339)},
			testReturn:    true,
			expectedEndTs: &currentTs,
			errStr:        "should return an end timestamp of now if the end time is from the future",
		},
		"futureTime": {
			testParams: timeTestParams{startTime: "2037-01-01T00:00:00Z", endTime: "2037-01-01T01:23:45Z"},
			testErr:    true,
			errStr:     "must return an error when startTime is in the future",
		},
		"reversedTime": {
			testParams: timeTestParams{startTime: "2024-07-16T18:00:00Z", endTime: "2024-07-15T14:30:00Z"},
			testErr:    true,
			errStr:     "must return an error when startTime is after endTime",
		},
		"startEndOnly": {
			testParams: timeTestParams{startTime: "2024-07-14T14:30:00Z", endTime: "2024-07-14T18:00:00Z"},
			testReturn: true,
			// Has to be set later because it needs to be a pointer
			// expectedPeriod: 3*time.Hour + 30*time.Minute
			errStr: "should return a period that is the difference between end time and start time",
		},
		"startPeriodOnly": {
			testParams: timeTestParams{startTime: "2024-07-14T11:50:00Z", period: 15 * time.Minute},
			testReturn: true,
			// Has to be set later because it needs to be a pointer
			// expectedEndTs: time.Parse(time.RFC3339, "2024-07-14T12:05:00Z"),
			errStr: "should return an end timestamp that is the start time plus the period",
		},
		"endPeriodOnly": {
			testParams: timeTestParams{endTime: "2024-07-14T18:00:00Z", period: 4 * time.Hour},
			testReturn: true,
			// Has to be set later because it needs to be a pointer
			// expectedStartTs: time.Parse(time.RFC3339, "2024-07-14T14:00:00Z")
			errStr: "should return a start timestamp that is the end time minus the period",
		},
		"tooManyTimes": {
			testParams: timeTestParams{startTime: "2024-07-15T12:34:56Z", endTime: "2024-07-15T12:45:36Z", period: 3 * 24 * time.Hour},
			testErr:    true,
			errStr:     "must return an error when all of startTime, endTime, and period are specified",
		},
	}

	startEndOnlyPeriod := 3*time.Hour + 30*time.Minute
	testTimes["startEndOnly"].expectedPeriod = &startEndOnlyPeriod

	startPeriodOnlyEndTs, _ := time.Parse(time.RFC3339, "2024-07-14T12:05:00Z")
	testTimes["startPeriodOnly"].expectedEndTs = &startPeriodOnlyEndTs

	endPeriodOnlyStartTs, _ := time.Parse(time.RFC3339, "2024-07-14T14:00:00Z")
	testTimes["endPeriodOnly"].expectedStartTs = &endPeriodOnlyStartTs

	for testName, testTime := range testTimes {
		if (!testTime.testErr && !testTime.testReturn) || (testTime.testErr && testTime.testReturn) {
			t.Errorf("TestGetRangeTimestamps: bad test configuration in test '%v': tests must have exactly one test condition", testName)
		}
		params := testTime.testParams
		startTs, endTs, period, err := getRangeTimestamps(params.startTime, params.endTime, params.period)
		// Run tests for errors
		if testTime.testErr && err == nil {
			t.Errorf("getRangeTimestamps(%v, %v, %v) %v", params.startTime, params.endTime, params.period, testTime.errStr)
		}
		// If we're not testing for errors and we got an error, that's a problem
		if !testTime.testErr && err != nil {
			t.Errorf("getRangeTimestamps(%v, %v, %v) unexpectedly returned an error %v", params.startTime, params.endTime, params.period, err)
		}

		// Run tests for return values
		if testTime.expectedStartTs != nil && startTs != *testTime.expectedStartTs {
			t.Errorf("getRangeTimestamps(%v, %v, %v) %v; expected %v got %v", params.startTime, params.endTime, params.period, testTime.errStr, *testTime.expectedStartTs, startTs)
		}
		if testTime.expectedEndTs != nil && endTs != *testTime.expectedEndTs {
			t.Errorf("getRangeTimestamps(%v, %v, %v) %v; expected %v got %v", params.startTime, params.endTime, params.period, testTime.errStr, *testTime.expectedEndTs, endTs)
		}
		if testTime.expectedPeriod != nil && period != *testTime.expectedPeriod {
			t.Errorf("getRangeTimestamps(%v, %v, %v) %v; expected %v got %v", params.startTime, params.endTime, params.period, testTime.errStr, *testTime.expectedPeriod, period)
		}
	}
}

type backoffTestParams struct {
	currentRetry uint
	minWaitSecs  uint
	maxWaitSecs  uint
	backoff      bool
}

type backoffTest struct {
	testParams     backoffTestParams
	testErr        bool
	testReturn     bool
	expectedResult time.Duration
	errStr         string
}

func TestPromRetryWait(t *testing.T) {
	setupLogging()

	backoffTests := map[string]*backoffTest{
		"retryCountNonZero": {
			testParams: backoffTestParams{currentRetry: 0, minWaitSecs: 1, maxWaitSecs: 15, backoff: false},
			testErr:    true,
			errStr:     "should return an error if retryCount < 1",
		},
		"minWaitNonZero": {
			testParams: backoffTestParams{currentRetry: 1, minWaitSecs: 0, maxWaitSecs: 15, backoff: false},
			testErr:    true,
			errStr:     "should return an error if minWait < 1",
		},
		"maxWaitNonZero": {
			testParams: backoffTestParams{currentRetry: 1, minWaitSecs: 1, maxWaitSecs: 0, backoff: false},
			testErr:    true,
			errStr:     "should return an error if maxWait < 1",
		},
		"minWaitNotEqualMax": {
			testParams: backoffTestParams{currentRetry: 1, minWaitSecs: 7, maxWaitSecs: 7, backoff: false},
			testErr:    true,
			errStr:     "should return an error if minWait = maxWait",
		},
		"minWaitGreaterThanMax": {
			testParams: backoffTestParams{currentRetry: 1, minWaitSecs: 15, maxWaitSecs: 1, backoff: false},
			testErr:    true,
			errStr:     "should return an error if minWait > maxWait",
		},
		"noBackoffFirstRetry": {
			testParams:     backoffTestParams{currentRetry: 1, minWaitSecs: 1, maxWaitSecs: 15, backoff: false},
			testReturn:     true,
			expectedResult: 1 * time.Second,
			errStr:         "should return a retry wait of 1s",
		},
		"noBackoffSecondRetry": {
			testParams:     backoffTestParams{currentRetry: 2, minWaitSecs: 1, maxWaitSecs: 15, backoff: false},
			testReturn:     true,
			expectedResult: 1 * time.Second,
			errStr:         "should return a retry wait of 1s",
		},
		"noBackoffLaterRetry": {
			testParams:     backoffTestParams{currentRetry: 27, minWaitSecs: 1, maxWaitSecs: 15, backoff: false},
			testReturn:     true,
			expectedResult: 1 * time.Second,
			errStr:         "should return a retry wait of 1s",
		},
		"backoffFirstRetry": {
			testParams:     backoffTestParams{currentRetry: 1, minWaitSecs: 1, maxWaitSecs: 15, backoff: true},
			testReturn:     true,
			expectedResult: 1 * time.Second,
			errStr:         "should return a retry wait of 1s",
		},
		"backoffSecondRetry": {
			testParams:     backoffTestParams{currentRetry: 2, minWaitSecs: 1, maxWaitSecs: 15, backoff: true},
			testReturn:     true,
			expectedResult: 2 * time.Second,
			errStr:         "should return a retry wait of 2s",
		},
		"backoffThirdRetry": {
			testParams:     backoffTestParams{currentRetry: 3, minWaitSecs: 1, maxWaitSecs: 15, backoff: true},
			testReturn:     true,
			expectedResult: 4 * time.Second,
			errStr:         "should return a retry wait of 4s",
		},
		"backoffLaterRetry": {
			testParams:     backoffTestParams{currentRetry: 27, minWaitSecs: 1, maxWaitSecs: 15, backoff: true},
			testReturn:     true,
			expectedResult: 15 * time.Second,
			errStr:         "should return a retry wait of 15s",
		},
	}

	for testName, backoffTest := range backoffTests {
		if (!backoffTest.testErr && !backoffTest.testReturn) || (backoffTest.testErr && backoffTest.testReturn) {
			t.Errorf("TestPromRetryWait: bad test configuration in test '%v': tests must have exactly one test condition", testName)
		}
		params := backoffTest.testParams
		result, err := promRetryWait(params.currentRetry, params.minWaitSecs, params.maxWaitSecs, params.backoff)
		if backoffTest.testErr && err == nil {
			t.Errorf("promRetryWait(%v, %v, %v, %v) %v", params.currentRetry, params.minWaitSecs, params.maxWaitSecs, params.backoff, backoffTest.errStr)
		}
		// If we're not testing for errors and we got an error, that's a problem
		if !backoffTest.testErr && err != nil {
			t.Errorf("promRetryWait(%v, %v, %v, %v) unexpectedly returned an error %v", params.currentRetry, params.minWaitSecs, params.maxWaitSecs, params.backoff, err)
		}

		// Run tests for return values
		if backoffTest.testReturn && result != backoffTest.expectedResult {
			t.Errorf("promRetryWait(%v, %v, %v, %v) %v; expected %v got %v", params.currentRetry, params.minWaitSecs, params.maxWaitSecs, params.backoff, backoffTest.errStr, backoffTest.expectedResult, result)
		}
	}
}
