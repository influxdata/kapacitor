package integrations

import (
	"net/http"
	"os"
	"path"
	"testing"
	"time"

	imodels "github.com/influxdb/influxdb/models"
	"github.com/influxdb/kapacitor"
	"github.com/influxdb/kapacitor/clock"
	"github.com/influxdb/kapacitor/wlog"
)

func TestBatch_SimpleMR(t *testing.T) {

	var script = `
batch
	.query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA'
''')
		.period(10s)
		.groupBy(time(2s), "cpu")
	.mapReduce(influxql.count("value"))
	.window()
		.period(20s)
		.every(20s)
	.mapReduce(influxql.sum("count"))
	.httpOut("TestBatch_SimpleMR")
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu-total"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1970, 1, 1, 0, 0, 19, 0, time.UTC),
					20.0,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu0"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1970, 1, 1, 0, 0, 19, 0, time.UTC),
					20.0,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu1"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1970, 1, 1, 0, 0, 19, 0, time.UTC),
					20.0,
				}},
			},
		},
	}

	clock, et, errCh, tm := testBatcher(t, "TestBatch_SimpleMR", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(30 * time.Second))
	// Wait till the replay has finished
	if e := <-errCh; e != nil {
		t.Error(e)
	}
	// Wait till the task is finished
	if e := et.Err(); e != nil {
		t.Error(e)
	}

	// Get the result
	output, err := et.GetOutput("TestBatch_SimpleMR")
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

// Helper test function for batcher
func testBatcher(t *testing.T, name, script string) (clock.Setter, *kapacitor.ExecutingTask, <-chan error, *kapacitor.TaskMaster) {
	if testing.Verbose() {
		wlog.LogLevel = wlog.DEBUG
	} else {
		wlog.LogLevel = wlog.OFF
	}

	// Create task
	task, err := kapacitor.NewBatcher(name, script)
	if err != nil {
		t.Fatal(err)
	}

	// Load test data
	data, err := os.Open(path.Join("data", name+".brpl"))
	if err != nil {
		t.Fatal(err)
	}
	c := clock.New(time.Unix(0, 0))
	r := kapacitor.NewReplay(c)

	// Create a new execution env
	tm := kapacitor.NewTaskMaster()
	tm.HTTPDService = httpService
	tm.Open()

	//Start the task
	et, err := tm.StartTask(task)
	if err != nil {
		t.Fatal(err)
	}

	// Replay test data to executor
	batch := tm.BatchCollector(name)
	errCh := r.ReplayBatch(data, batch)

	t.Log(string(et.Task.Dot()))
	return r.Setter, et, errCh, tm
}
