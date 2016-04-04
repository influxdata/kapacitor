package integrations

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"testing"
	"time"

	imodels "github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/clock"
	"github.com/influxdata/kapacitor/wlog"
)

func TestBatch_Derivative(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT sum("value") as "value"
		FROM "telegraf"."default".packets
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))
	|derivative('value')
	|httpOut('TestBatch_Derivative')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						0.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						0.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						0.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						0.5,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Derivative", script, 21*time.Second, er)
}

func TestBatch_DerivativeUnit(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT sum("value") as "value"
		FROM "telegraf"."default".packets
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))
	|derivative('value')
		.unit(2s)
	|httpOut('TestBatch_Derivative')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						1.0,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Derivative", script, 21*time.Second, er)
}

func TestBatch_DerivativeN(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT sum("value") as "value"
		FROM "telegraf"."default".packets
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))
	|derivative('value')
	|httpOut('TestBatch_DerivativeNN')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						0.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						0.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						-501.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						0.5,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_DerivativeNN", script, 21*time.Second, er)
}

func TestBatch_DerivativeNN(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT sum("value") as "value"
		FROM "telegraf"."default".packets
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))
	|derivative('value')
		.nonNegative()
	|httpOut('TestBatch_DerivativeNN')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						0.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						0.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						0.5,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_DerivativeNN", script, 21*time.Second, er)
}

func TestBatch_SimpleMR(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s), 'cpu')
	|count('mean')
	|window()
		.period(20s)
		.every(20s)
	|sum('count')
	|httpOut('TestBatch_SimpleMR')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu-total"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					10.0,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu0"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					10.0,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu1"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er)
}

func TestBatch_DoubleGroupBy(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA' AND "cpu" != 'cpu-total'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s), 'cpu')
	|groupBy()
	|max('mean')
	|httpOut('TestBatch_SimpleMR')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 18, 0, time.UTC),
					95.98484848485191,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er)
}

func TestBatch_Join(t *testing.T) {

	var script = `
var cpu0 = batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu0'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))

var cpu1 = batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu1'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))

cpu0
	|join(cpu1)
		.as('cpu0', 'cpu1')
	|count('cpu0.mean')
	|window()
		.period(20s)
		.every(20s)
	|sum('count')
	|httpOut('TestBatch_Join')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Join", script, 30*time.Second, er)
}

func TestBatch_JoinTolerance(t *testing.T) {

	var script = `
var cpu0 = batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu0'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))

var cpu1 = batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu1'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))

cpu0
	|join(cpu1)
		.as('cpu0', 'cpu1')
		.tolerance(1s)
	|count('cpu0.mean')
	|window()
		.period(20s)
		.every(20s)
	|sum('count')
	|httpOut('TestBatch_JoinTolerance')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_JoinTolerance", script, 30*time.Second, er)
}

// Helper test function for batcher
func testBatcher(t *testing.T, name, script string) (clock.Setter, *kapacitor.ExecutingTask, <-chan error, *kapacitor.TaskMaster) {
	if testing.Verbose() {
		wlog.SetLevel(wlog.DEBUG)
	} else {
		wlog.SetLevel(wlog.OFF)
	}

	// Create a new execution env
	tm := kapacitor.NewTaskMaster(logService)
	tm.HTTPDService = httpService
	tm.TaskStore = taskStore{}
	tm.DeadmanService = deadman{}
	tm.Open()

	// Create task
	task, err := tm.NewTask(name, script, kapacitor.BatchTask, dbrps, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Load test data
	var allData []io.ReadCloser
	var data io.ReadCloser
	for i := 0; err == nil; {
		f := fmt.Sprintf("%s.%d.brpl", name, i)
		data, err = os.Open(path.Join("data", f))
		if err == nil {
			allData = append(allData, data)
			i++
		}
	}
	if len(allData) == 0 {
		t.Fatal("could not find any data files for", name)
	}

	// Use 1971 so that we don't get true negatives on Epoch 0 collisions
	c := clock.New(time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC))
	r := kapacitor.NewReplay(c)

	//Start the task
	et, err := tm.StartTask(task)
	if err != nil {
		t.Fatal(err)
	}

	// Replay test data to executor
	batches := tm.BatchCollectors(name)
	replayErr := r.ReplayBatch(allData, batches, false)

	t.Log(string(et.Task.Dot()))
	return r.Setter, et, replayErr, tm
}

func testBatcherWithOutput(
	t *testing.T,
	name,
	script string,
	duration time.Duration,
	er kapacitor.Result,
	ignoreOrder ...bool,
) {
	clock, et, replayErr, tm := testBatcher(t, name, script)
	defer tm.Close()

	err := fastForwardTask(clock, et, replayErr, tm, duration)
	if err != nil {
		t.Error(err)
	}

	// Get the result
	output, err := et.GetOutput(name)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(output.Endpoint())
	if err != nil {
		t.Fatal(err)
	}

	// Assert we got the expected result
	result := kapacitor.ResultFromJSON(resp.Body)
	if eq, msg := compareResults(er, result); !eq {
		t.Error(msg)
	}
}
