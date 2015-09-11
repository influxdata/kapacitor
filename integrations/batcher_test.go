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

func TestBatchingData(t *testing.T) {
	var script = `
batch
	.query('''select "idle" from "tests"."default".cpu where "host" = 'serverA' ''')
	.period(10s)
	.groupBy(time(2s))
	.cache();
`

	expectedResult := executor.Result{}

	testBatcher(t, "TestBatchingData", script, expectedResult)
}

func TestSplitBatchData(t *testing.T) {

	var script = `
var cpu = batch
	.query('''select "idle" from "tests"."default".cpu where dc = 'nyc' ''')
	.period(10s)
	.groupBy(time(2s));

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

	testStreamer(t, "TestSplitBatchData", script, expectedResult)
}

func TestJoinBatchData(t *testing.T) {

	var script = `
var errorCounts = batch
			.query('''select count("value") from "tests"."default"."errors"''')
			.period(10s)
			.groupBy(time(5s), "service");

var viewCounts = batch
			.query('''select count("value") from "tests"."default"."errors"''')
			.period(10s)
			.groupBy(time(5s), "service");

errorCounts.join(viewCounts)
		.as("errors", "views")
		//No need for a map phase
		.reduce(expr("error_percent", "errors.count / views.count"), "*")
		.cache();
`

	expectedResult := executor.Result{}

	testStreamer(t, "TestJoinBatchData", script, expectedResult)
}

func TestUnionBatchData(t *testing.T) {

	var script = `
var cpu = batch
			.query('''select mean("idle") from "tests"."default"."errors"''')
			.period(10s)
			.groupBy(time(5s))
var mem = batch
			.query('''select mean("free") from "tests"."default"."errors"''')
			.period(10s)
			.groupBy(time(5s))
var disk = batch
			.query('''select mean("iops") from "tests"."default"."errors"''')
			.period(10s)
			.groupBy(time(5s))

cpu.union(mem, disk)
		.cache();
`

	expectedResult := executor.Result{}

	testStreamer(t, "TestUnionBatchData", script, expectedResult)
}

func TestBatchingAlert(t *testing.T) {
	var script = `
batch
	.query('''select percentile("idle", 10) as p10 from "tests"."default".cpu where "host" = 'serverA' ''')
	.period(10s)
	.groupBy(time(2s))
	.where("p10 < 30")
	.alert()
	.post("http://localhost");
`

	expectedResult := executor.Result{}

	testBatcher(t, "TestBatchingAlert", script, expectedResult)
}

func TestBatchingAlertFlapping(t *testing.T) {
	var script = `
batch
	.query('''select percentile("idle", 10) as p10 from "tests"."default".cpu where "host" = 'serverA' ''')
	.period(10s)
	.groupBy(time(2s))
	.where("p10 < 30")
	.alert()
	.flapping(25, 50)
	.post("http://localhost");
`

	expectedResult := executor.Result{}

	testBatcher(t, "TestBatchingAlertFlapping", script, expectedResult)
}

// Helper test function for batcher
func testBatcher(t *testting.T, name, script string, expectedResult interface{}) {
	assert := assert.New(t)

	// Create task
	t := task.NewBatcher(name, script)

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
