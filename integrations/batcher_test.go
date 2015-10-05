package integrations

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"testing"
	"time"

	"github.com/influxdb/kapacitor"
	"github.com/influxdb/kapacitor/clock"
	"github.com/influxdb/kapacitor/models"
	"github.com/stretchr/testify/assert"
)

func TestBatchingData(t *testing.T) {
	assert := assert.New(t)

	var script = `
batch
	.query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA'
''')
		.period(10s)
		.groupBy(time(2s), "cpu")
	.mapReduce(influxql.count, "value")
	.window()
		.period(20s)
		.every(20s)
	.mapReduce(influxql.sum, "count")
	.httpOut("TestBatchingData");
`

	er := kapacitor.Result{
		Window: map[models.GroupID][]*models.Point{
			"cpu=cpu-total,": {
				{
					Name:   "cpu_usage_idle",
					Tags:   map[string]string{"cpu": "cpu-total"},
					Fields: map[string]interface{}{"sum": 20.0},
					Time:   time.Date(1970, 1, 1, 0, 0, 19, 0, time.UTC),
				},
			},
			"cpu=cpu0,": {
				{
					Name:   "cpu_usage_idle",
					Tags:   map[string]string{"cpu": "cpu0"},
					Fields: map[string]interface{}{"sum": 20.0},
					Time:   time.Date(1970, 1, 1, 0, 0, 19, 0, time.UTC),
				},
			},
			"cpu=cpu1,": {
				{
					Name:   "cpu_usage_idle",
					Tags:   map[string]string{"cpu": "cpu1"},
					Fields: map[string]interface{}{"sum": 20.0},
					Time:   time.Date(1970, 1, 1, 0, 0, 19, 0, time.UTC),
				},
			},
		},
	}

	clock, et, errCh, tm := testBatcher(t, "TestBatchingData", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(30 * time.Second))
	// Wait till the replay has finished
	assert.Nil(<-errCh)
	// Wait till the task is finished
	assert.Nil(et.Err())

	// Get the result
	output, err := et.GetOutput("TestBatchingData")
	if !assert.Nil(err) {
		t.FailNow()
	}

	resp, err := http.Get(output.Endpoint())
	if !assert.Nil(err) {
		t.FailNow()
	}

	// Assert we got the expected result
	result := kapacitor.ResultFromJSON(resp.Body)
	if assert.Equal(len(er.Window), len(result.Window)) {
		for g := range er.Window {
			if assert.Equal(len(er.Window[g]), len(result.Window[g])) {
				for i := range er.Window[g] {
					assert.Equal(er.Window[g][i].Name, result.Window[g][i].Name, "g: %s i: %d", g, i)
					assert.Equal(er.Window[g][i].Tags, result.Window[g][i].Tags, "g: %s i: %d", g, i)
					assert.Equal(er.Window[g][i].Fields, result.Window[g][i].Fields, "g: %s i: %d", g, i)
					assert.True(
						er.Window[g][i].Time.Equal(result.Window[g][i].Time),
						"g: %s i: %d %s != %s",
						g,
						i,
						er.Window[g][i].Time, result.Window[g][i].Time,
					)
				}
			}
		}
	}
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
	//er := kapacitor.Result{}

	testBatcher(t, "TestSplitBatchData", script)
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

	//er := kapacitor.Result{}

	testBatcher(t, "TestJoinBatchData", script)
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

	//er := kapacitor.Result{}

	testBatcher(t, "TestUnionBatchData", script)
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

	//er := kapacitor.Result{}

	testBatcher(t, "TestBatchingAlert", script)
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

	//er := kapacitor.Result{}

	testBatcher(t, "TestBatchingAlertFlapping", script)
}

// Helper test function for batcher
func testBatcher(t *testing.T, name, script string) (clock.Setter, *kapacitor.ExecutingTask, <-chan error, *kapacitor.TaskMaster) {
	assert := assert.New(t)

	// Create task
	task, err := kapacitor.NewBatcher(name, script)
	if !assert.Nil(err) {
		t.FailNow()
	}

	// Load test data
	data, err := os.Open(path.Join("data", name+".rpl"))
	if !assert.Nil(err) {
		t.FailNow()
	}
	c := clock.New(time.Unix(0, 0))
	r := kapacitor.NewReplay(c)

	// Create a new execution env
	tm := kapacitor.NewTaskMaster()
	tm.HTTPDService = httpService
	tm.Open()

	//Start the task
	et, err := tm.StartTask(task)
	if !assert.Nil(err) {
		t.FailNow()
	}

	// Replay test data to executor
	batch := tm.BatchCollector(name)
	errCh := r.ReplayBatch(data, batch)

	fmt.Fprintln(os.Stderr, string(et.Task.Dot()))
	return r.Setter, et, errCh, tm
}
