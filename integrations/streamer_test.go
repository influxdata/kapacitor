package integrations

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/influxdb/kapacitor/executor"
	"github.com/influxdb/kapacitor/replay"
	"github.com/influxdb/kapacitor/task"
	"github.com/stretchr/testify/assert"
)

func TestWindowDataByTime(t *testing.T) {

	var script = `
stream
	.from("cpu")
	.where("host = 'serverA'")
	.window()
		.period(1s)
		.every(1s)
	.cache();
`

	expectedResult := executor.Result{}

	testStreamer(t, "TestWindowDataByTime", script, expectedResult)
}

func TestSplitStreamData(t *testing.T) {

	var script = `
var cpu = stream
	.from("cpu")
	.where("dc = 'nyc'");

cpu
	.where("host = 'serverA'");
	.window()
		.period(1s)
		.every(1s)
	.cache("/a");

cpu
	.where("host = 'serverB'");
	.window()
		.period(1s)
		.every(1s)
	.cache("/b");
`
	expectedResult := executor.Result{}

	testStreamer(t, "TestSplitStreamData", script, expectedResult)
}

func TestJoinStreamData(t *testing.T) {

	var script = `
var errorCounts = stream
			.from("errors")
			.groupBy("service")
			.window()
				.period(10s)
				.every(10s)
			.mapReduce(influxql.count, "value");

var viewCounts = stream
			.from("views")
			.groupBy("service")
			.window()
				.period(10s)
				.every(10s)
			.mapReduce(influxql.count, "value");

errorCounts.join(viewCounts)
		.as("errors", "views")
		.map(expr("error_percent", "errors.count / views.count"), "*")
		.cache();
`

	expectedResult := executor.Result{}

	testStreamer(t, "TestJoinStreamData", script, expectedResult)
}

func TestUnionStreamData(t *testing.T) {

	var script = `
var cpu = stream
			.from("cpu")
var mem = stream
			.from("memory")
var disk = stream
			.from("disk")

cpu.union(mem, disk)
		.window()
			.period(10s)
			.every(10s)
		.cache();
`

	expectedResult := executor.Result{}

	testStreamer(t, "TestUnionStreamData", script, expectedResult)
}

func TestStreamAggregations(t *testing.T) {
	assert := assert.New(t)

	type testCase struct {
		Method         string
		ExpectedResult executor.Result
	}

	var scriptTmpl = `
var fMapReduce = {{ .Map }};
stream
	.from("cpu")
	.where("host = 'serverA'")
	.window()
		.period(1s)
		.every(1s)
	.mapReduce(fMapReduce, "idle")
	.cache();
`

	testCases := []testCase{
		testCase{
			Method:         "influxql.sum",
			ExpectedResult: executor.Result{},
		},
		testCase{
			Method:         "influxql.count",
			ExpectedResult: executor.Result{},
		},
		testCase{
			Method:         "influxql.min",
			ExpectedResult: executor.Result{},
		},
		testCase{
			Method:         "influxql.max",
			ExpectedResult: executor.Result{},
		},
		testCase{
			Method:         "influxql.median",
			ExpectedResult: executor.Result{},
		},
		testCase{
			Method:         "influxql.mean",
			ExpectedResult: executor.Result{},
		},
	}

	tmpl, err := template.New("script").Parse(scriptTmpl)
	if !assert.Nil(err) {
		t.FailNow()
	}

	for _, tc := range testCases {
		var script bytes.Buffer
		tmpl.Execute(script, tc)
		testStreamer(t, "TestStreamAggregation:"+tc.Method, string(script.Bytes()), tc.ExpectedResult)
	}
}

func TestCustomFunctions(t *testing.T) {
	var script = `
var fMap = loadMapFunc("./TestCustomMapFunction.py");
var fReduce = loadReduceFunc("./TestCustomReduceFunction.py");
stream
	.from("cpu")
	.where("host = 'serverA'")
	.window()
		.period(1s)
		.every(1s)
	.map(fMap, "idle")
	.reduce(fReduce)
	.cache();
`

	expectedResult := executor.Result{}

	testStreamer(t, "TestCustomFunctions", script, expectedResult)
}

func TestCustomMRFunction(t *testing.T) {
	var script = `
var fMapReduce = loadMapReduceFunc("./TestCustomMapReduceFunction.py");
stream
	.from("cpu")
	.where("host = 'serverA'")
	.window()
		.period(1s)
		.every(1s)
	.mapReduce(fMap, "idle")
	.cache();
`

	expectedResult := executor.Result{}

	testStreamer(t, "TestCustomMRFunction", script, expectedResult)
}

func TestStreamingAlert(t *testing.T) {
	var script = `
stream
	.from("cpu")
	.where("host = 'serverA'")
	.window()
		.period(1s)
		.every(1s)
	.mapReduce(influxql.percentile(10), "idle")
	.where("percentile < 30")
	.alert()
	.post("http://localhost");
`

	expectedResult := executor.Result{}

	testBatcher(t, "TestStreamingAlert", script, expectedResult)
}

func TestBatchingAlertFlapping(t *testing.T) {
	var script = `
stream
	.from("cpu")
	.where("host = 'serverA'")
	.window()
		.period(1s)
		.every(1s)
	.mapReduce(influxql.percentile(10), "idle")
	.where("percentile < 30")
	.alert()
	.flapping(25, 50)
	.post("http://localhost");
`

	expectedResult := executor.Result{}

	testBatcher(t, "TestStreamingAlertFlapping", script, expectedResult)
}

// Helper test function for streamer
func testStreamer(t *testting.T, name, script string, expectedResult interface{}) {
	assert := assert.New(t)

	//Create the task
	t := task.NewStreamer(name, script)

	// Load test data
	data, err := os.Open(name + ".dat")
	if !assert.Nil(err) {
		t.FailNow()
	}
	defer data.Close()
	r := replay.New()
	r.Load(name, data)

	// Create a new execution env
	e, err := executor.NewExecutor()
	if !assert.Nil(err) {
		t.FailNow()
	}
	defer e.Shutdown()

	//Start the task
	rt, err := e.StartTask(t)
	if !assert.Nil(err) {
		t.FailNow()
	}
	defer rt.Stop()

	// Replay test data to executor
	r.Replay(name, e)

	// Get the result
	output, err := rt.GetOutput(name)
	if !assert.Nil(err) {
		t.FailNow()
	}
	resp, err := http.Get(output.Endpoint())
	if !assert.Nil(err) {
		t.FailNow()
	}

	//Assert we got the expected result
	result := decodeResult(resp)
	expectedResult := executor.Result{}
	assert.Equal(expectedResult, result)

}
