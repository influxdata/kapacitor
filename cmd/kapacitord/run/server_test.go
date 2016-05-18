package run_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	iclient "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/toml"
	"github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/cmd/kapacitord/run"
	"github.com/influxdata/kapacitor/services/udf"
)

func TestServer_Ping(t *testing.T) {
	s, cli := OpenDefaultServer()
	t.Log(s.URL())
	defer s.Close()
	_, version, err := cli.Ping()
	if err != nil {
		t.Fatal(err)
	}
	if version != "testServer" {
		t.Fatal("unexpected version", version)
	}
}

func TestServer_CreateTask(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTaskID"
	ttype := client.StreamTask
	dbrps := []client.DBRP{
		{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		},
		{
			Database:        "otherdb",
			RetentionPolicy: "default",
		},
	}
	tick := `stream
    |from()
        .measurement('test')
`
	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	ti, err := cli.Task(task.Link, nil)
	if err != nil {
		t.Fatal(err)
	}

	if ti.Error != "" {
		t.Fatal(ti.Error)
	}
	if ti.ID != id {
		t.Fatalf("unexpected id got %s exp %s", ti.ID, id)
	}
	if ti.Type != client.StreamTask {
		t.Fatalf("unexpected type got %v exp %v", ti.Type, client.StreamTask)
	}
	if ti.Status != client.Disabled {
		t.Fatalf("unexpected status got %v exp %v", ti.Status, client.Disabled)
	}
	if !reflect.DeepEqual(ti.DBRPs, dbrps) {
		t.Fatalf("unexpected dbrps got %s exp %s", ti.DBRPs, dbrps)
	}
	if ti.TICKscript != tick {
		t.Fatalf("unexpected TICKscript got %s exp %s", ti.TICKscript, tick)
	}
	dot := "digraph testTaskID {\nstream0 -> from1;\n}"
	if ti.Dot != dot {
		t.Fatalf("unexpected dot\ngot\n%s\nexp\n%s\n", ti.Dot, dot)
	}
}

func TestServer_EnableTask(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTaskID"
	ttype := client.StreamTask
	dbrps := []client.DBRP{
		{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		},
		{
			Database:        "otherdb",
			RetentionPolicy: "default",
		},
	}
	tick := `stream
    |from()
        .measurement('test')
`
	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	ti, err := cli.Task(task.Link, nil)
	if err != nil {
		t.Fatal(err)
	}

	if ti.Error != "" {
		t.Fatal(ti.Error)
	}
	if ti.ID != id {
		t.Fatalf("unexpected id got %s exp %s", ti.ID, id)
	}
	if ti.Type != client.StreamTask {
		t.Fatalf("unexpected type got %v exp %v", ti.Type, client.StreamTask)
	}
	if ti.Status != client.Enabled {
		t.Fatalf("unexpected status got %v exp %v", ti.Status, client.Enabled)
	}
	if ti.Executing != true {
		t.Fatalf("unexpected executing got %v exp %v", ti.Executing, true)
	}
	if !reflect.DeepEqual(ti.DBRPs, dbrps) {
		t.Fatalf("unexpected dbrps got %s exp %s", ti.DBRPs, dbrps)
	}
	if ti.TICKscript != tick {
		t.Fatalf("unexpected TICKscript got %s exp %s", ti.TICKscript, tick)
	}
	dot := `digraph testTaskID {
graph [throughput="0.00 points/s"];

stream0 [avg_exec_time_ns="0" ];
stream0 -> from1 [processed="0"];

from1 [avg_exec_time_ns="0" ];
}`
	if ti.Dot != dot {
		t.Fatalf("unexpected dot\ngot\n%s\nexp\n%s\n", ti.Dot, dot)
	}
}

func TestServer_EnableTaskOnCreate(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTaskID"
	ttype := client.StreamTask
	dbrps := []client.DBRP{
		{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		},
		{
			Database:        "otherdb",
			RetentionPolicy: "default",
		},
	}
	tick := `stream
    |from()
        .measurement('test')
`
	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	ti, err := cli.Task(task.Link, nil)
	if err != nil {
		t.Fatal(err)
	}

	if ti.Error != "" {
		t.Fatal(ti.Error)
	}
	if ti.ID != id {
		t.Fatalf("unexpected id got %s exp %s", ti.ID, id)
	}
	if ti.Type != client.StreamTask {
		t.Fatalf("unexpected type got %v exp %v", ti.Type, client.StreamTask)
	}
	if ti.Status != client.Enabled {
		t.Fatalf("unexpected status got %v exp %v", ti.Status, client.Enabled)
	}
	if ti.Executing != true {
		t.Fatalf("unexpected executing got %v exp %v", ti.Executing, true)
	}
	if !reflect.DeepEqual(ti.DBRPs, dbrps) {
		t.Fatalf("unexpected dbrps got %s exp %s", ti.DBRPs, dbrps)
	}
	if ti.TICKscript != tick {
		t.Fatalf("unexpected TICKscript got %s exp %s", ti.TICKscript, tick)
	}
	dot := `digraph testTaskID {
graph [throughput="0.00 points/s"];

stream0 [avg_exec_time_ns="0" ];
stream0 -> from1 [processed="0"];

from1 [avg_exec_time_ns="0" ];
}`
	if ti.Dot != dot {
		t.Fatalf("unexpected dot\ngot\n%s\nexp\n%s\n", ti.Dot, dot)
	}
}

func TestServer_DisableTask(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTaskID"
	ttype := client.StreamTask
	dbrps := []client.DBRP{
		{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		},
		{
			Database:        "otherdb",
			RetentionPolicy: "default",
		},
	}
	tick := `stream
    |from()
        .measurement('test')
`
	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	ti, err := cli.Task(task.Link, nil)
	if err != nil {
		t.Fatal(err)
	}

	if ti.Error != "" {
		t.Fatal(ti.Error)
	}
	if ti.ID != id {
		t.Fatalf("unexpected id got %s exp %s", ti.ID, id)
	}
	if ti.Type != client.StreamTask {
		t.Fatalf("unexpected type got %v exp %v", ti.Type, client.StreamTask)
	}
	if ti.Status != client.Disabled {
		t.Fatalf("unexpected status got %v exp %v", ti.Status, client.Disabled)
	}
	if !reflect.DeepEqual(ti.DBRPs, dbrps) {
		t.Fatalf("unexpected dbrps got %s exp %s", ti.DBRPs, dbrps)
	}
	if ti.TICKscript != tick {
		t.Fatalf("unexpected TICKscript got %s exp %s", ti.TICKscript, tick)
	}
	dot := "digraph testTaskID {\nstream0 -> from1;\n}"
	if ti.Dot != dot {
		t.Fatalf("unexpected dot\ngot\n%s\nexp\n%s\n", ti.Dot, dot)
	}
}

func TestServer_DeleteTask(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTaskID"
	ttype := client.StreamTask
	dbrps := []client.DBRP{
		{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		},
		{
			Database:        "otherdb",
			RetentionPolicy: "default",
		},
	}
	tick := `stream
    |from()
        .measurement('test')
`
	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cli.DeleteTask(task.Link)
	if err != nil {
		t.Fatal(err)
	}

	ti, err := cli.Task(task.Link, nil)
	if err == nil {
		t.Fatal("unexpected task:", ti)
	}
}

func TestServer_ListTasks(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()
	count := 10

	ttype := client.StreamTask
	tick := `stream
    |from()
        .measurement('test')
`

	dbrps := []client.DBRP{
		{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		},
		{
			Database:        "otherdb",
			RetentionPolicy: "default",
		},
	}
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("testTaskID%d", i)
		status := client.Disabled
		if i%2 == 0 {
			status = client.Enabled
		}
		_, err := cli.CreateTask(client.CreateTaskOptions{
			ID:         id,
			Type:       ttype,
			DBRPs:      dbrps,
			TICKscript: tick,
			Status:     status,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	tasks, err := cli.ListTasks(nil)
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := count, len(tasks); exp != got {
		t.Fatalf("unexpected number of tasks: exp:%d got:%d", exp, got)
	}
	for i, task := range tasks {
		if exp, got := fmt.Sprintf("testTaskID%d", i), task.ID; exp != got {
			t.Errorf("unexpected task.ID i:%d exp:%s got:%s", i, exp, got)
		}
		if exp, got := client.StreamTask, task.Type; exp != got {
			t.Errorf("unexpected task.Type i:%d exp:%v got:%v", i, exp, got)
		}
		if !reflect.DeepEqual(task.DBRPs, dbrps) {
			t.Fatalf("unexpected dbrps i:%d exp:%s got:%s", i, dbrps, task.DBRPs)
		}
		exp := client.Disabled
		if i%2 == 0 {
			exp = client.Enabled
		}
		if got := task.Status; exp != got {
			t.Errorf("unexpected task.Status i:%d exp:%v got:%v", i, exp, got)
		}
		if exp, got := i%2 == 0, task.Executing; exp != got {
			t.Errorf("unexpected task.Executing i:%d exp:%v got:%v", i, exp, got)
		}
		if exp, got := true, len(task.Dot) != 0; exp != got {
			t.Errorf("unexpected task.Dot i:%d exp:\n%v\ngot:\n%v\n", i, exp, got)
		}
		if exp, got := tick, task.TICKscript; exp != got {
			t.Errorf("unexpected task.TICKscript i:%d exp:%v got:%v", i, exp, got)
		}
		if exp, got := "", task.Error; exp != got {
			t.Errorf("unexpected task.Error i:%d exp:%v got:%v", i, exp, got)
		}
	}

}

func TestServer_ListTasks_Fields(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()
	count := 100

	ttype := client.StreamTask
	tick := `stream
    |from()
        .measurement('test')
`
	dbrps := []client.DBRP{
		{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		},
		{
			Database:        "otherdb",
			RetentionPolicy: "default",
		},
	}
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("testTaskID%d", i)
		_, err := cli.CreateTask(client.CreateTaskOptions{
			ID:         id,
			Type:       ttype,
			DBRPs:      dbrps,
			TICKscript: tick,
			Status:     client.Enabled,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	tasks, err := cli.ListTasks(&client.ListTasksOptions{
		Pattern: "testTaskID1*",
		Fields:  []string{"type", "status"},
		Offset:  1,
		Limit:   5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := 5, len(tasks); exp != got {
		t.Fatalf("unexpected number of tasks: exp:%d got:%d", exp, got)
	}
	for i, task := range tasks {
		if exp, got := fmt.Sprintf("testTaskID1%d", i), task.ID; exp != got {
			t.Errorf("unexpected task.ID i:%d exp:%s got:%s", i, exp, got)
		}
		if exp, got := client.StreamTask, task.Type; exp != got {
			t.Errorf("unexpected task.Type i:%d exp:%v got:%v", i, exp, got)
		}
		if exp, got := client.Enabled, task.Status; exp != got {
			t.Errorf("unexpected task.Status i:%d exp:%v got:%v", i, exp, got)
		}
		// We didn't request these fields so they should be default zero values
		if exp, got := 0, len(task.DBRPs); exp != got {
			t.Fatalf("unexpected dbrps i:%d exp:%d got:%d", i, exp, got)
		}
		if exp, got := false, task.Executing; exp != got {
			t.Errorf("unexpected task.Executing i:%d exp:%v got:%v", i, exp, got)
		}
		if exp, got := "", task.Dot; exp != got {
			t.Errorf("unexpected task.Dot i:%d exp:%v got:%v", i, exp, got)
		}
		if exp, got := "", task.TICKscript; exp != got {
			t.Errorf("unexpected task.TICKscript i:%d exp:%v got:%v", i, exp, got)
		}
		if exp, got := "", task.Error; exp != got {
			t.Errorf("unexpected task.Error i:%d exp:%v got:%v", i, exp, got)
		}
	}

}

func TestServer_StreamTask(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testStreamTask"
	ttype := client.StreamTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	tick := `stream
    |from()
        .measurement('test')
    |window()
        .period(10s)
        .every(10s)
    |count('value')
    |httpOut('count')
`

	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), id)

	// Request data before any writes and expect null responses
	nullResponse := `{}`
	err = s.HTTPGetRetry(endpoint, nullResponse, 100, time.Millisecond*5)
	if err != nil {
		t.Error(err)
	}

	points := `test value=1 0000000000
test value=1 0000000001
test value=1 0000000001
test value=1 0000000002
test value=1 0000000002
test value=1 0000000003
test value=1 0000000003
test value=1 0000000004
test value=1 0000000005
test value=1 0000000005
test value=1 0000000005
test value=1 0000000006
test value=1 0000000007
test value=1 0000000008
test value=1 0000000009
test value=1 0000000010
test value=1 0000000011
`
	v := url.Values{}
	v.Add("precision", "s")
	s.MustWrite("mydb", "myrp", points, v)

	exp := `{"series":[{"name":"test","columns":["time","count"],"values":[["1970-01-01T00:00:10Z",15]]}]}`
	err = s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5)
	if err != nil {
		t.Error(err)
	}
}

func TestServer_StreamTask_AllMeasurements(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testStreamTask"
	ttype := client.StreamTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	tick := `stream
    |from()
    |window()
        .period(10s)
        .every(10s)
    |count('value')
    |httpOut('count')
`

	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), id)

	// Request data before any writes and expect null responses
	nullResponse := `{}`
	err = s.HTTPGetRetry(endpoint, nullResponse, 100, time.Millisecond*5)
	if err != nil {
		t.Error(err)
	}

	points := `test0 value=1 0000000000
test1 value=1 0000000001
test0 value=1 0000000001
test1 value=1 0000000002
test0 value=1 0000000002
test1 value=1 0000000003
test0 value=1 0000000003
test1 value=1 0000000004
test0 value=1 0000000005
test1 value=1 0000000005
test0 value=1 0000000005
test1 value=1 0000000006
test0 value=1 0000000007
test1 value=1 0000000008
test0 value=1 0000000009
test1 value=1 0000000010
test0 value=1 0000000011
`
	v := url.Values{}
	v.Add("precision", "s")
	s.MustWrite("mydb", "myrp", points, v)

	exp := `{"series":[{"name":"test0","columns":["time","count"],"values":[["1970-01-01T00:00:10Z",15]]}]}`
	err = s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5)
	if err != nil {
		t.Error(err)
	}
}

func TestServer_BatchTask(t *testing.T) {
	c := NewConfig()
	c.InfluxDB[0].Enabled = true
	count := 0
	db := NewInfluxDB(func(q string) *iclient.Response {
		if len(q) > 6 && q[:6] == "SELECT" {
			count++
			return &iclient.Response{
				Results: []iclient.Result{{
					Series: []models.Row{{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 1, int(time.Millisecond), time.UTC).Format(time.RFC3339Nano),
								1.0,
							},
							{
								time.Date(1971, 1, 1, 0, 0, 1, 2*int(time.Millisecond), time.UTC).Format(time.RFC3339Nano),
								1.0,
							},
						},
					}},
				}},
			}
		}
		return nil
	})
	c.InfluxDB[0].URLs = []string{db.URL()}
	s := OpenServer(c)
	defer s.Close()
	cli := Client(s)

	id := "testBatchTask"
	ttype := client.BatchTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	tick := `batch
    |query(' SELECT value from mydb.myrp.cpu ')
        .period(5ms)
        .every(5ms)
    |count('value')
    |httpOut('count')
`

	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), id)

	exp := `{"series":[{"name":"cpu","columns":["time","count"],"values":[["1971-01-01T00:00:01.002Z",2]]}]}`
	err = s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5)
	if err != nil {
		t.Error(err)
	}
	err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	if count == 0 {
		t.Error("unexpected query count", count)
	}
}

func TestServer_InvalidBatchTask(t *testing.T) {
	c := NewConfig()
	c.InfluxDB[0].Enabled = true
	db := NewInfluxDB(func(q string) *iclient.Response {
		return nil
	})
	c.InfluxDB[0].URLs = []string{db.URL()}
	s := OpenServer(c)
	defer s.Close()
	cli := Client(s)

	id := "testInvalidBatchTask"
	ttype := client.BatchTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	tick := `batch
    |query(' SELECT value from unknowndb.unknownrp.cpu ')
        .period(5ms)
        .every(5ms)
    |count('value')
    |httpOut('count')
`

	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	expErr := `batch query is not allowed to request data from "unknowndb"."unknownrp"`
	if err != nil && err.Error() != expErr {
		t.Fatalf("unexpected err: got %v exp %s", err, expErr)
	}

	err = cli.DeleteTask(task.Link)
	if err != nil {
		t.Fatal(err)
	}
}

func TestServer_RecordReplayStream(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testStreamTask"
	ttype := client.StreamTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}

	tmpDir, err := ioutil.TempDir("", "testStreamTaskRecording")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	tick := `stream
    |from()
        .measurement('test')
    |window()
        .period(10s)
        .every(10s)
    |count('value')
    |alert()
        .id('test-count')
        .message('{{ .ID }} got: {{ index .Fields "count" }}')
        .crit(lambda: TRUE)
        .log('` + tmpDir + `/alert.log')
`

	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}
	recording, err := cli.RecordStream(client.RecordStreamOptions{
		ID:   "recordingid",
		Task: task.ID,
		Stop: time.Date(1970, 1, 1, 0, 0, 10, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "/kapacitor/v1/recordings/recordingid", recording.Link.Href; exp != got {
		t.Errorf("unexpected recording.Link.Href got %s exp %s", got, exp)
	}

	points := `test value=1 0000000000
test value=1 0000000001
test value=1 0000000001
test value=1 0000000002
test value=1 0000000002
test value=1 0000000003
test value=1 0000000003
test value=1 0000000004
test value=1 0000000005
test value=1 0000000005
test value=1 0000000005
test value=1 0000000006
test value=1 0000000007
test value=1 0000000008
test value=1 0000000009
test value=1 0000000010
test value=1 0000000011
test value=1 0000000012
`
	v := url.Values{}
	v.Add("precision", "s")
	s.MustWrite("mydb", "myrp", points, v)

	retry := 0
	for recording.Status == client.Running {
		time.Sleep(100 * time.Millisecond)
		recording, err = cli.Recording(recording.Link)
		if err != nil {
			t.Fatal(err)
		}
		retry++
		if retry > 10 {
			t.Fatal("failed to finish recording")
		}
	}
	if recording.Status != client.Finished || recording.Error != "" {
		t.Errorf("recording failed: %s", recording.Error)
	}

	replay, err := cli.CreateReplay(client.CreateReplayOptions{
		ID:            "replayid",
		Task:          id,
		Recording:     recording.ID,
		Clock:         client.Fast,
		RecordingTime: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "/kapacitor/v1/replays/replayid", replay.Link.Href; exp != got {
		t.Errorf("unexpected replay.Link.Href got %s exp %s", got, exp)
	}
	if exp, got := id, replay.Task; exp != got {
		t.Errorf("unexpected replay.Task got %s exp %s", got, exp)
	}

	retry = 0
	for replay.Status == client.Running {
		time.Sleep(100 * time.Millisecond)
		replay, err = cli.Replay(replay.Link)
		if err != nil {
			t.Fatal(err)
		}
		retry++
		if retry > 10 {
			t.Fatal("failed to finish replay")
		}
	}
	if replay.Status != client.Finished || replay.Error != "" {
		t.Errorf("replay failed: %s", replay.Error)
	}

	f, err := os.Open(path.Join(tmpDir, "alert.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	type response struct {
		ID      string          `json:"id"`
		Message string          `json:"message"`
		Time    time.Time       `json:"time"`
		Level   string          `json:"level"`
		Data    influxql.Result `json:"data"`
	}
	exp := response{
		ID:      "test-count",
		Message: "test-count got: 15",
		Time:    time.Date(1970, 1, 1, 0, 0, 10, 0, time.UTC),
		Level:   "CRITICAL",
		Data: influxql.Result{
			Series: models.Rows{
				{
					Name:    "test",
					Columns: []string{"time", "count"},
					Values: [][]interface{}{
						{
							time.Date(1970, 1, 1, 0, 0, 10, 0, time.UTC).Format(time.RFC3339Nano),
							15.0,
						},
					},
				},
			},
		},
	}
	got := response{}
	d := json.NewDecoder(f)
	d.Decode(&got)
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got, exp)
	}

	recordings, err := cli.ListRecordings(nil)
	if exp, got := 1, len(recordings); exp != got {
		t.Fatalf("unexpected recordings list:\ngot %v\nexp %v", got, exp)
	}

	err = cli.DeleteRecording(recordings[0].Link)
	if err != nil {
		t.Error(err)
	}

	recordings, err = cli.ListRecordings(nil)
	if exp, got := 0, len(recordings); exp != got {
		t.Errorf("unexpected recordings list:\ngot %v\nexp %v", got, exp)
	}

	replays, err := cli.ListReplays(nil)
	if exp, got := 1, len(replays); exp != got {
		t.Fatalf("unexpected replays list:\ngot %v\nexp %v", got, exp)
	}

	err = cli.DeleteReplay(replays[0].Link)
	if err != nil {
		t.Error(err)
	}

	replays, err = cli.ListReplays(nil)
	if exp, got := 0, len(replays); exp != got {
		t.Errorf("unexpected replays list:\ngot %v\nexp %v", got, exp)
	}
}

func TestServer_RecordReplayBatch(t *testing.T) {
	c := NewConfig()
	c.InfluxDB[0].Enabled = true
	value := 0
	db := NewInfluxDB(func(q string) *iclient.Response {
		if len(q) > 6 && q[:6] == "SELECT" {
			r := &iclient.Response{
				Results: []iclient.Result{{
					Series: []models.Row{{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, value, 0, time.UTC).Format(time.RFC3339Nano),
								float64(value),
							},
							{
								time.Date(1971, 1, 1, 0, 0, value+1, 0, time.UTC).Format(time.RFC3339Nano),
								float64(value + 1),
							},
						},
					}},
				}},
			}
			value += 2
			return r
		}
		return nil
	})
	c.InfluxDB[0].URLs = []string{db.URL()}
	s := OpenServer(c)
	defer s.Close()
	cli := Client(s)

	id := "testBatchTask"
	ttype := client.BatchTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}

	tmpDir, err := ioutil.TempDir("", "testBatchTaskRecording")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	tick := `batch
    |query('SELECT value from mydb.myrp.cpu')
        .period(2s)
        .every(2s)
    |alert()
        .id('test-batch')
        .message('{{ .ID }} got: {{ index .Fields "value" }}')
        .crit(lambda: "value" > 2.0)
        .log('` + tmpDir + `/alert.log')
`

	_, err = cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	recording, err := cli.RecordBatch(client.RecordBatchOptions{
		ID:    "recordingid",
		Task:  id,
		Start: time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
		Stop:  time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "/kapacitor/v1/recordings/recordingid", recording.Link.Href; exp != got {
		t.Errorf("unexpected recording.Link.Href got %s exp %s", got, exp)
	}
	// Wait for recording to finish.
	retry := 0
	for recording.Status == client.Running {
		time.Sleep(100 * time.Millisecond)
		recording, err = cli.Recording(recording.Link)
		if err != nil {
			t.Fatal(err)
		}
		retry++
		if retry > 10 {
			t.Fatal("failed to perfom recording")
		}
	}

	replay, err := cli.CreateReplay(client.CreateReplayOptions{
		Task:          id,
		Recording:     recording.ID,
		Clock:         client.Fast,
		RecordingTime: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := id, replay.Task; exp != got {
		t.Errorf("unexpected replay.Task got %s exp %s", got, exp)
	}

	// Wait for replay to finish.
	retry = 0
	for replay.Status == client.Running {
		time.Sleep(100 * time.Millisecond)
		replay, err = cli.Replay(replay.Link)
		if err != nil {
			t.Fatal(err)
		}
		retry++
		if retry > 10 {
			t.Fatal("failed to perfom replay")
		}
	}

	f, err := os.Open(path.Join(tmpDir, "alert.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	type response struct {
		ID      string          `json:"id"`
		Message string          `json:"message"`
		Time    time.Time       `json:"time"`
		Level   string          `json:"level"`
		Data    influxql.Result `json:"data"`
	}
	exp := []response{
		{
			ID:      "test-batch",
			Message: "test-batch got: 3",
			Time:    time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
			Level:   "CRITICAL",
			Data: influxql.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC).Format(time.RFC3339Nano),
								2.0,
							},
							{
								time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC).Format(time.RFC3339Nano),
								3.0,
							},
						},
					},
				},
			},
		},
		{
			ID:      "test-batch",
			Message: "test-batch got: 4",
			Time:    time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
			Level:   "CRITICAL",
			Data: influxql.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC).Format(time.RFC3339Nano),
								4.0,
							},
							{
								time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC).Format(time.RFC3339Nano),
								5.0,
							},
						},
					},
				},
			},
		},
	}
	scanner := bufio.NewScanner(f)
	got := make([]response, 0)
	g := response{}
	for scanner.Scan() {
		json.Unmarshal(scanner.Bytes(), &g)
		got = append(got, g)
	}
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got, exp)
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got[0].Data.Series[0], exp[0].Data.Series[0])
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got[1].Data.Series[0], exp[1].Data.Series[0])
	}

	recordings, err := cli.ListRecordings(nil)
	if exp, got := 1, len(recordings); exp != got {
		t.Fatalf("unexpected recordings list:\ngot %v\nexp %v", got, exp)
	}

	err = cli.DeleteRecording(recordings[0].Link)
	if err != nil {
		t.Error(err)
	}

	recordings, err = cli.ListRecordings(nil)
	if exp, got := 0, len(recordings); exp != got {
		t.Errorf("unexpected recordings list:\ngot %v\nexp %v", got, exp)
	}

	replays, err := cli.ListReplays(nil)
	if exp, got := 1, len(replays); exp != got {
		t.Fatalf("unexpected replays list:\ngot %v\nexp %v", got, exp)
	}

	err = cli.DeleteReplay(replays[0].Link)
	if err != nil {
		t.Error(err)
	}

	replays, err = cli.ListReplays(nil)
	if exp, got := 0, len(replays); exp != got {
		t.Errorf("unexpected replays list:\ngot %v\nexp %v", got, exp)
	}
}
func TestServer_ReplayBatch(t *testing.T) {
	c := NewConfig()
	c.InfluxDB[0].Enabled = true
	value := 0
	db := NewInfluxDB(func(q string) *iclient.Response {
		if len(q) > 6 && q[:6] == "SELECT" {
			r := &iclient.Response{
				Results: []iclient.Result{{
					Series: []models.Row{{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, value, 0, time.UTC).Format(time.RFC3339Nano),
								float64(value),
							},
							{
								time.Date(1971, 1, 1, 0, 0, value+1, 0, time.UTC).Format(time.RFC3339Nano),
								float64(value + 1),
							},
						},
					}},
				}},
			}
			value += 2
			return r
		}
		return nil
	})
	c.InfluxDB[0].URLs = []string{db.URL()}
	s := OpenServer(c)
	defer s.Close()
	cli := Client(s)

	id := "testBatchTask"
	ttype := client.BatchTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}

	tmpDir, err := ioutil.TempDir("", "testBatchTaskRecording")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	tick := `batch
    |query('SELECT value from mydb.myrp.cpu')
        .period(2s)
        .every(2s)
    |alert()
        .id('test-batch')
        .message('{{ .ID }} got: {{ index .Fields "value" }}')
        .crit(lambda: "value" > 2.0)
        .log('` + tmpDir + `/alert.log')
`

	_, err = cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	replay, err := cli.ReplayBatch(client.ReplayBatchOptions{
		ID:            "replayid",
		Task:          id,
		Start:         time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
		Stop:          time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
		Clock:         client.Fast,
		RecordingTime: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "/kapacitor/v1/replays/replayid", replay.Link.Href; exp != got {
		t.Errorf("unexpected replay.Link.Href got %s exp %s", got, exp)
	}
	// Wait for replay to finish.
	retry := 0
	for replay.Status == client.Running {
		time.Sleep(100 * time.Millisecond)
		replay, err = cli.Replay(replay.Link)
		if err != nil {
			t.Fatal(err)
		}
		retry++
		if retry > 10 {
			t.Fatal("failed to perfom replay")
		}
	}

	f, err := os.Open(path.Join(tmpDir, "alert.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	type response struct {
		ID      string          `json:"id"`
		Message string          `json:"message"`
		Time    time.Time       `json:"time"`
		Level   string          `json:"level"`
		Data    influxql.Result `json:"data"`
	}
	exp := []response{
		{
			ID:      "test-batch",
			Message: "test-batch got: 3",
			Time:    time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
			Level:   "CRITICAL",
			Data: influxql.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC).Format(time.RFC3339Nano),
								2.0,
							},
							{
								time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC).Format(time.RFC3339Nano),
								3.0,
							},
						},
					},
				},
			},
		},
		{
			ID:      "test-batch",
			Message: "test-batch got: 4",
			Time:    time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
			Level:   "CRITICAL",
			Data: influxql.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC).Format(time.RFC3339Nano),
								4.0,
							},
							{
								time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC).Format(time.RFC3339Nano),
								5.0,
							},
						},
					},
				},
			},
		},
	}
	scanner := bufio.NewScanner(f)
	got := make([]response, 0)
	g := response{}
	for scanner.Scan() {
		json.Unmarshal(scanner.Bytes(), &g)
		got = append(got, g)
	}
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got, exp)
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got[0].Data.Series[0], exp[0].Data.Series[0])
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got[1].Data.Series[0], exp[1].Data.Series[0])
	}

	recordings, err := cli.ListRecordings(nil)
	if exp, got := 0, len(recordings); exp != got {
		t.Fatalf("unexpected recordings list:\ngot %v\nexp %v", got, exp)
	}

	replays, err := cli.ListReplays(nil)
	if exp, got := 1, len(replays); exp != got {
		t.Fatalf("unexpected replays list:\ngot %v\nexp %v", got, exp)
	}

	err = cli.DeleteReplay(replays[0].Link)
	if err != nil {
		t.Error(err)
	}

	replays, err = cli.ListReplays(nil)
	if exp, got := 0, len(replays); exp != got {
		t.Errorf("unexpected replays list:\ngot %v\nexp %v", got, exp)
	}
}

func TestServer_RecordReplayQuery(t *testing.T) {
	c := NewConfig()
	c.InfluxDB[0].Enabled = true
	db := NewInfluxDB(func(q string) *iclient.Response {
		if len(q) > 6 && q[:6] == "SELECT" {
			r := &iclient.Response{
				Results: []iclient.Result{{
					Series: []models.Row{
						{
							Name:    "cpu",
							Columns: []string{"time", "value"},
							Values: [][]interface{}{
								{
									time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
									0.0,
								},
								{
									time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC).Format(time.RFC3339Nano),
									1.0,
								},
							},
						},
						{
							Name:    "cpu",
							Columns: []string{"time", "value"},
							Values: [][]interface{}{
								{
									time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC).Format(time.RFC3339Nano),
									2.0,
								},
								{
									time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC).Format(time.RFC3339Nano),
									3.0,
								},
							},
						},
						{
							Name:    "cpu",
							Columns: []string{"time", "value"},
							Values: [][]interface{}{
								{
									time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC).Format(time.RFC3339Nano),
									4.0,
								},
								{
									time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC).Format(time.RFC3339Nano),
									5.0,
								},
							},
						},
					},
				}},
			}
			return r
		}
		return nil
	})
	c.InfluxDB[0].URLs = []string{db.URL()}
	s := OpenServer(c)
	defer s.Close()
	cli := Client(s)

	id := "testBatchTask"
	ttype := client.BatchTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}

	tmpDir, err := ioutil.TempDir("", "testBatchTaskRecording")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	tick := `batch
    |query('SELECT value from mydb.myrp.cpu')
        .period(2s)
        .every(2s)
    |alert()
        .id('test-batch')
        .message('{{ .ID }} got: {{ index .Fields "value" }}')
        .crit(lambda: "value" > 2.0)
        .log('` + tmpDir + `/alert.log')
`

	_, err = cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	recording, err := cli.RecordQuery(client.RecordQueryOptions{
		ID:    "recordingid",
		Query: "SELECT value from mydb.myrp.cpu",
		Type:  client.BatchTask,
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "/kapacitor/v1/recordings/recordingid", recording.Link.Href; exp != got {
		t.Errorf("unexpected recording.Link.Href got %s exp %s", got, exp)
	}
	// Wait for recording to finish.
	retry := 0
	for recording.Status == client.Running {
		time.Sleep(100 * time.Millisecond)
		recording, err = cli.Recording(recording.Link)
		if err != nil {
			t.Fatal(err)
		}
		retry++
		if retry > 10 {
			t.Fatal("failed to perfom recording")
		}
	}

	replay, err := cli.CreateReplay(client.CreateReplayOptions{
		Task:          id,
		Recording:     recording.ID,
		Clock:         client.Fast,
		RecordingTime: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := id, replay.Task; exp != got {
		t.Errorf("unexpected replay.Task got %s exp %s", got, exp)
	}

	// Wait for replay to finish.
	retry = 0
	for replay.Status == client.Running {
		time.Sleep(100 * time.Millisecond)
		replay, err = cli.Replay(replay.Link)
		if err != nil {
			t.Fatal(err)
		}
		retry++
		if retry > 10 {
			t.Fatal("failed to perfom replay")
		}
	}

	f, err := os.Open(path.Join(tmpDir, "alert.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	type response struct {
		ID      string          `json:"id"`
		Message string          `json:"message"`
		Time    time.Time       `json:"time"`
		Level   string          `json:"level"`
		Data    influxql.Result `json:"data"`
	}
	exp := []response{
		{
			ID:      "test-batch",
			Message: "test-batch got: 3",
			Time:    time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
			Level:   "CRITICAL",
			Data: influxql.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC).Format(time.RFC3339Nano),
								2.0,
							},
							{
								time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC).Format(time.RFC3339Nano),
								3.0,
							},
						},
					},
				},
			},
		},
		{
			ID:      "test-batch",
			Message: "test-batch got: 4",
			Time:    time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
			Level:   "CRITICAL",
			Data: influxql.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC).Format(time.RFC3339Nano),
								4.0,
							},
							{
								time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC).Format(time.RFC3339Nano),
								5.0,
							},
						},
					},
				},
			},
		},
	}
	scanner := bufio.NewScanner(f)
	got := make([]response, 0)
	g := response{}
	for scanner.Scan() {
		json.Unmarshal(scanner.Bytes(), &g)
		got = append(got, g)
	}
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got, exp)
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got[0].Data.Series[0], exp[0].Data.Series[0])
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got[1].Data.Series[0], exp[1].Data.Series[0])
	}

	// ------------
	// Test List/Delete Recordings/Replays

	recordings, err := cli.ListRecordings(nil)
	if exp, got := 1, len(recordings); exp != got {
		t.Fatalf("unexpected recordings list:\ngot %v\nexp %v", got, exp)
	}

	// Test List Recordings via direct default URL
	resp, err := http.Get(s.URL() + "/recordings")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if exp, got := http.StatusOK, resp.StatusCode; exp != got {
		t.Errorf("unexpected status code, got %d exp %d", got, exp)
	}
	// Response type
	type recResponse struct {
		Recordings []client.Recording `json:"recordings"`
	}
	dec := json.NewDecoder(resp.Body)
	recR := recResponse{}
	dec.Decode(&recR)
	if exp, got := 1, len(recR.Recordings); exp != got {
		t.Fatalf("unexpected recordings count, got %d exp %d", got, exp)
	}

	err = cli.DeleteRecording(recordings[0].Link)
	if err != nil {
		t.Error(err)
	}

	recordings, err = cli.ListRecordings(nil)
	if exp, got := 0, len(recordings); exp != got {
		t.Errorf("unexpected recordings list:\ngot %v\nexp %v", got, exp)
	}

	replays, err := cli.ListReplays(nil)
	if exp, got := 1, len(replays); exp != got {
		t.Fatalf("unexpected replays list:\ngot %v\nexp %v", got, exp)
	}

	// Test List Replays via direct default URL
	resp, err = http.Get(s.URL() + "/replays")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if exp, got := http.StatusOK, resp.StatusCode; exp != got {
		t.Errorf("unexpected status code, got %d exp %d", got, exp)
	}
	// Response type
	type repResponse struct {
		Replays []client.Replay `json:"replays"`
	}
	dec = json.NewDecoder(resp.Body)
	repR := repResponse{}
	dec.Decode(&repR)
	if exp, got := 1, len(repR.Replays); exp != got {
		t.Fatalf("unexpected replays count, got %d exp %d", got, exp)
	}

	err = cli.DeleteReplay(replays[0].Link)
	if err != nil {
		t.Error(err)
	}

	replays, err = cli.ListReplays(nil)
	if exp, got := 0, len(replays); exp != got {
		t.Errorf("unexpected replays list:\ngot %v\nexp %v", got, exp)
	}
}

func TestServer_ReplayQuery(t *testing.T) {
	c := NewConfig()
	c.InfluxDB[0].Enabled = true
	db := NewInfluxDB(func(q string) *iclient.Response {
		if len(q) > 6 && q[:6] == "SELECT" {
			r := &iclient.Response{
				Results: []iclient.Result{{
					Series: []models.Row{
						{
							Name:    "cpu",
							Columns: []string{"time", "value"},
							Values: [][]interface{}{
								{
									time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
									0.0,
								},
								{
									time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC).Format(time.RFC3339Nano),
									1.0,
								},
							},
						},
						{
							Name:    "cpu",
							Columns: []string{"time", "value"},
							Values: [][]interface{}{
								{
									time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC).Format(time.RFC3339Nano),
									2.0,
								},
								{
									time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC).Format(time.RFC3339Nano),
									3.0,
								},
							},
						},
						{
							Name:    "cpu",
							Columns: []string{"time", "value"},
							Values: [][]interface{}{
								{
									time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC).Format(time.RFC3339Nano),
									4.0,
								},
								{
									time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC).Format(time.RFC3339Nano),
									5.0,
								},
							},
						},
					},
				}},
			}
			return r
		}
		return nil
	})
	c.InfluxDB[0].URLs = []string{db.URL()}
	s := OpenServer(c)
	defer s.Close()
	cli := Client(s)

	id := "testBatchTask"
	ttype := client.BatchTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}

	tmpDir, err := ioutil.TempDir("", "testBatchTaskRecording")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	tick := `batch
    |query('SELECT value from mydb.myrp.cpu')
        .period(2s)
        .every(2s)
    |alert()
        .id('test-batch')
        .message('{{ .ID }} got: {{ index .Fields "value" }}')
        .crit(lambda: "value" > 2.0)
        .log('` + tmpDir + `/alert.log')
`

	_, err = cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	replay, err := cli.ReplayQuery(client.ReplayQueryOptions{
		ID:            "replayid",
		Query:         "SELECT value from mydb.myrp.cpu",
		Task:          id,
		Clock:         client.Fast,
		RecordingTime: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "/kapacitor/v1/replays/replayid", replay.Link.Href; exp != got {
		t.Errorf("unexpected replay.Link.Href got %s exp %s", got, exp)
	}
	// Wait for replay to finish.
	retry := 0
	for replay.Status == client.Running {
		time.Sleep(100 * time.Millisecond)
		replay, err = cli.Replay(replay.Link)
		if err != nil {
			t.Fatal(err)
		}
		retry++
		if retry > 10 {
			t.Fatal("failed to perfom replay")
		}
	}

	f, err := os.Open(path.Join(tmpDir, "alert.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	type response struct {
		ID      string          `json:"id"`
		Message string          `json:"message"`
		Time    time.Time       `json:"time"`
		Level   string          `json:"level"`
		Data    influxql.Result `json:"data"`
	}
	exp := []response{
		{
			ID:      "test-batch",
			Message: "test-batch got: 3",
			Time:    time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
			Level:   "CRITICAL",
			Data: influxql.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC).Format(time.RFC3339Nano),
								2.0,
							},
							{
								time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC).Format(time.RFC3339Nano),
								3.0,
							},
						},
					},
				},
			},
		},
		{
			ID:      "test-batch",
			Message: "test-batch got: 4",
			Time:    time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
			Level:   "CRITICAL",
			Data: influxql.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC).Format(time.RFC3339Nano),
								4.0,
							},
							{
								time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC).Format(time.RFC3339Nano),
								5.0,
							},
						},
					},
				},
			},
		},
	}
	scanner := bufio.NewScanner(f)
	got := make([]response, 0)
	g := response{}
	for scanner.Scan() {
		json.Unmarshal(scanner.Bytes(), &g)
		got = append(got, g)
	}
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got, exp)
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got[0].Data.Series[0], exp[0].Data.Series[0])
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got[1].Data.Series[0], exp[1].Data.Series[0])
	}

	recordings, err := cli.ListRecordings(nil)
	if exp, got := 0, len(recordings); exp != got {
		t.Fatalf("unexpected recordings list:\ngot %v\nexp %v", got, exp)
	}

	replays, err := cli.ListReplays(nil)
	if exp, got := 1, len(replays); exp != got {
		t.Fatalf("unexpected replays list:\ngot %v\nexp %v", got, exp)
	}

	err = cli.DeleteReplay(replays[0].Link)
	if err != nil {
		t.Error(err)
	}

	replays, err = cli.ListReplays(nil)
	if exp, got := 0, len(replays); exp != got {
		t.Errorf("unexpected replays list:\ngot %v\nexp %v", got, exp)
	}
}

// If this test fails due to missing python dependencies, run 'INSTALL_PREFIX=/usr/local ./install-deps.sh' from the root directory of the
// kapacitor project.
func TestServer_UDFStreamAgents(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	udfDir := filepath.Clean(filepath.Join(dir, "../../../udf"))

	tdir, err := ioutil.TempDir("", "kapacitor_server_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tdir)

	agents := []struct {
		buildFunc func() error
		config    udf.FunctionConfig
	}{
		// Go
		{
			buildFunc: func() error {
				// Explicitly compile the binary.
				// We could just use 'go run' but I ran into race conditions
				// where 'go run' was not handing off to the compiled process in time
				// and I didn't care to dig into 'go run's specific behavior.
				cmd := exec.Command(
					"go",
					"build",
					"-o",
					filepath.Join(tdir, "movavg"),
					filepath.Join(udfDir, "agent/examples/moving_avg/moving_avg.go"),
				)
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Log(string(out))
					return err
				}
				return nil
			},
			config: udf.FunctionConfig{
				Prog:    filepath.Join(tdir, "movavg"),
				Timeout: toml.Duration(time.Minute),
			},
		},
		// Python
		{
			buildFunc: func() error { return nil },
			config: udf.FunctionConfig{
				Prog:    "python2",
				Args:    []string{"-u", filepath.Join(udfDir, "agent/examples/moving_avg/moving_avg.py")},
				Timeout: toml.Duration(time.Minute),
				Env: map[string]string{
					"PYTHONPATH": strings.Join(
						[]string{filepath.Join(udfDir, "agent/py"), os.Getenv("PYTHONPATH")},
						string(filepath.ListSeparator),
					),
				},
			},
		},
	}
	for _, agent := range agents {
		err := agent.buildFunc()
		if err != nil {
			t.Fatal(err)
		}
		c := NewConfig()
		c.UDF.Functions = map[string]udf.FunctionConfig{
			"movingAvg": agent.config,
		}
		testStreamAgent(t, c)
	}
}

func testStreamAgent(t *testing.T, c *run.Config) {
	s := NewServer(c)
	err := s.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cli := Client(s)

	id := "testUDFTask"
	ttype := client.StreamTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	tick := `stream
    |from()
        .measurement('test')
        .groupBy('group')
    @movingAvg()
        .field('value')
        .size(10)
        .as('mean')
    |window()
        .period(11s)
        .every(11s)
    |last('mean').as('mean')
    |httpOut('moving_avg')
`

	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("%s/tasks/%s/moving_avg", s.URL(), id)

	// Request data before any writes and expect null responses
	nullResponse := `{}`
	err = s.HTTPGetRetry(endpoint, nullResponse, 100, time.Millisecond*5)
	if err != nil {
		t.Error(err)
	}

	points := `test,group=a value=1 0000000000
test,group=b value=2 0000000000
test,group=a value=1 0000000001
test,group=b value=2 0000000001
test,group=a value=1 0000000002
test,group=b value=2 0000000002
test,group=a value=1 0000000003
test,group=b value=2 0000000003
test,group=a value=1 0000000004
test,group=b value=2 0000000004
test,group=a value=1 0000000005
test,group=b value=2 0000000005
test,group=a value=1 0000000006
test,group=b value=2 0000000006
test,group=a value=1 0000000007
test,group=b value=2 0000000007
test,group=a value=1 0000000008
test,group=b value=2 0000000008
test,group=a value=1 0000000009
test,group=b value=2 0000000009
test,group=a value=0 0000000010
test,group=b value=1 0000000010
test,group=a value=0 0000000011
test,group=b value=0 0000000011
`
	v := url.Values{}
	v.Add("precision", "s")
	s.MustWrite("mydb", "myrp", points, v)

	exp := `{"series":[{"name":"test","tags":{"group":"a"},"columns":["time","mean"],"values":[["1970-01-01T00:00:11Z",0.9]]},{"name":"test","tags":{"group":"b"},"columns":["time","mean"],"values":[["1970-01-01T00:00:11Z",1.9]]}]}`
	err = s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5)
	if err != nil {
		t.Error(err)
	}
}

// If this test fails due to missing python dependencies, run 'INSTALL_PREFIX=/usr/local ./install-deps.sh' from the root directory of the
// kapacitor project.
func TestServer_UDFStreamAgentsSocket(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	udfDir := filepath.Clean(filepath.Join(dir, "../../../udf"))

	tdir, err := ioutil.TempDir("", "kapacitor_server_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tdir)

	agents := []struct {
		startFunc func() *exec.Cmd
		config    udf.FunctionConfig
	}{
		// Go
		{
			startFunc: func() *exec.Cmd {
				cmd := exec.Command(
					"go",
					"build",
					"-o",
					filepath.Join(tdir, "mirror"),
					filepath.Join(udfDir, "agent/examples/mirror/mirror.go"),
				)
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatal(string(out))
				}
				cmd = exec.Command(
					filepath.Join(tdir, "mirror"),
					"-socket",
					filepath.Join(tdir, "mirror.go.sock"),
				)
				cmd.Stderr = os.Stderr
				return cmd
			},
			config: udf.FunctionConfig{
				Socket:  filepath.Join(tdir, "mirror.go.sock"),
				Timeout: toml.Duration(time.Minute),
			},
		},
		// Python
		{
			startFunc: func() *exec.Cmd {
				cmd := exec.Command(
					"python2",
					"-u",
					filepath.Join(udfDir, "agent/examples/mirror/mirror.py"),
					filepath.Join(tdir, "mirror.py.sock"),
				)
				cmd.Stderr = os.Stderr
				env := os.Environ()
				env = append(env, fmt.Sprintf(
					"%s=%s",
					"PYTHONPATH",
					strings.Join(
						[]string{filepath.Join(udfDir, "agent/py"), os.Getenv("PYTHONPATH")},
						string(filepath.ListSeparator),
					),
				))
				cmd.Env = env
				return cmd
			},
			config: udf.FunctionConfig{
				Socket:  filepath.Join(tdir, "mirror.py.sock"),
				Timeout: toml.Duration(time.Minute),
			},
		},
	}
	for _, agent := range agents {
		cmd := agent.startFunc()
		cmd.Start()
		defer cmd.Process.Signal(os.Interrupt)
		if err != nil {
			t.Fatal(err)
		}
		c := NewConfig()
		c.UDF.Functions = map[string]udf.FunctionConfig{
			"mirror": agent.config,
		}
		testStreamAgentSocket(t, c)
	}
}

func testStreamAgentSocket(t *testing.T, c *run.Config) {
	s := NewServer(c)
	err := s.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cli := Client(s)

	id := "testUDFTask"
	ttype := client.StreamTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	tick := `stream
    |from()
        .measurement('test')
        .groupBy('group')
    @mirror()
    |window()
        .period(10s)
        .every(10s)
    |count('value')
    |httpOut('count')
`

	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), id)

	// Request data before any writes and expect null responses
	nullResponse := `{}`
	err = s.HTTPGetRetry(endpoint, nullResponse, 100, time.Millisecond*5)
	if err != nil {
		t.Error(err)
	}

	points := `test,group=a value=1 0000000000
test,group=a value=1 0000000001
test,group=a value=1 0000000002
test,group=a value=1 0000000003
test,group=a value=1 0000000004
test,group=a value=1 0000000005
test,group=a value=1 0000000006
test,group=a value=1 0000000007
test,group=a value=1 0000000008
test,group=a value=1 0000000009
test,group=a value=0 0000000010
test,group=a value=0 0000000011
`
	v := url.Values{}
	v.Add("precision", "s")
	s.MustWrite("mydb", "myrp", points, v)

	exp := `{"series":[{"name":"test","tags":{"group":"a"},"columns":["time","count"],"values":[["1970-01-01T00:00:10Z",10]]}]}`
	err = s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5)
	if err != nil {
		t.Error(err)
	}
}

// If this test fails due to missing python dependencies, run 'INSTALL_PREFIX=/usr/local ./install-deps.sh' from the root directory of the
// kapacitor project.
func TestServer_UDFBatchAgents(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	udfDir := filepath.Clean(filepath.Join(dir, "../../../udf"))

	tdir, err := ioutil.TempDir("", "kapacitor_server_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tdir)

	agents := []struct {
		buildFunc func() error
		config    udf.FunctionConfig
	}{
		// Go
		{
			buildFunc: func() error {
				// Explicitly compile the binary.
				// We could just use 'go run' but I ran into race conditions
				// where 'go run' was not handing off to the compiled process in time
				// and I didn't care to dig into 'go run's specific behavior.
				cmd := exec.Command(
					"go",
					"build",
					"-o",
					filepath.Join(tdir, "outliers"),
					filepath.Join(udfDir, "agent/examples/outliers/outliers.go"),
				)
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Log(string(out))
					return err
				}
				return nil
			},
			config: udf.FunctionConfig{
				Prog:    filepath.Join(tdir, "outliers"),
				Timeout: toml.Duration(time.Minute),
			},
		},
		// Python
		{
			buildFunc: func() error { return nil },
			config: udf.FunctionConfig{
				Prog:    "python2",
				Args:    []string{"-u", filepath.Join(udfDir, "agent/examples/outliers/outliers.py")},
				Timeout: toml.Duration(time.Minute),
				Env: map[string]string{
					"PYTHONPATH": strings.Join(
						[]string{filepath.Join(udfDir, "agent/py"), os.Getenv("PYTHONPATH")},
						string(filepath.ListSeparator),
					),
				},
			},
		},
	}
	for _, agent := range agents {
		err := agent.buildFunc()
		if err != nil {
			t.Fatal(err)
		}
		c := NewConfig()
		c.UDF.Functions = map[string]udf.FunctionConfig{
			"outliers": agent.config,
		}
		testBatchAgent(t, c)
	}
}

func testBatchAgent(t *testing.T, c *run.Config) {
	count := 0
	db := NewInfluxDB(func(q string) *iclient.Response {
		if len(q) > 6 && q[:6] == "SELECT" {
			count++
			data := []float64{
				5,
				6,
				7,
				13,
				33,
				35,
				36,
				45,
				46,
				47,
				48,
				50,
				51,
				52,
				53,
				54,
				80,
				85,
				90,
				100,
			}
			// Shuffle data using count as seed.
			// Data order should not effect the result.
			r := rand.New(rand.NewSource(int64(count)))
			for i := range data {
				j := r.Intn(i + 1)
				data[i], data[j] = data[j], data[i]
			}

			// Create set values with time from shuffled data.
			values := make([][]interface{}, len(data))
			for i, value := range data {
				values[i] = []interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, (i+1)*int(time.Millisecond), time.UTC).Format(time.RFC3339Nano),
					value,
				}
			}

			return &iclient.Response{
				Results: []iclient.Result{{
					Series: []models.Row{{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Tags: map[string]string{
							"count": strconv.FormatInt(int64(count%2), 10),
						},
						Values: values,
					}},
				}},
			}
		}
		return nil
	})
	c.InfluxDB[0].URLs = []string{db.URL()}
	c.InfluxDB[0].Enabled = true
	s := NewServer(c)
	err := s.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cli := Client(s)

	id := "testUDFTask"
	ttype := client.BatchTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	tick := `batch
    |query(' SELECT value from mydb.myrp.cpu ')
        .period(5ms)
        .every(5ms)
        .groupBy('count')
    @outliers()
        .field('value')
        .scale(1.5)
    |count('value')
    |httpOut('count')
`

	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), id)
	exp := `{"series":[{"name":"cpu","tags":{"count":"1"},"columns":["time","count"],"values":[["1971-01-01T00:00:00.02Z",5]]},{"name":"cpu","tags":{"count":"0"},"columns":["time","count"],"values":[["1971-01-01T00:00:00.02Z",5]]}]}`
	err = s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*50)
	if err != nil {
		t.Error(err)
	}
	err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	if count == 0 {
		t.Error("unexpected query count", count)
	}
}

func TestServer_CreateTask_Defaults(t *testing.T) {
	s, cli := OpenDefaultServer()
	baseURL := s.URL()

	body := `
{
    "id" : "TASK_ID",
    "type" : "stream",
    "dbrps": [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
    "script": "stream\n    |from()\n        .measurement('cpu')\n"
}`
	resp, err := http.Post(baseURL+"/tasks", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if exp, got := http.StatusOK, resp.StatusCode; exp != got {
		t.Errorf("unexpected status code, got %d exp %d", got, exp)
	}

	id := "TASK_ID"
	tick := "stream\n    |from()\n        .measurement('cpu')\n"
	dbrps := []client.DBRP{
		{
			Database:        "DATABASE_NAME",
			RetentionPolicy: "RP_NAME",
		},
	}
	ti, err := cli.Task(cli.TaskLink(id), nil)
	if err != nil {
		t.Fatal(err)
	}

	if ti.Error != "" {
		t.Fatal(ti.Error)
	}
	if ti.ID != id {
		t.Fatalf("unexpected id got %s exp %s", ti.ID, id)
	}
	if ti.Type != client.StreamTask {
		t.Fatalf("unexpected type got %v exp %v", ti.Type, client.StreamTask)
	}
	if ti.Status != client.Disabled {
		t.Fatalf("unexpected status got %v exp %v", ti.Status, client.Disabled)
	}
	if !reflect.DeepEqual(ti.DBRPs, dbrps) {
		t.Fatalf("unexpected dbrps got %s exp %s", ti.DBRPs, dbrps)
	}
	if ti.TICKscript != tick {
		t.Fatalf("unexpected TICKscript got %s exp %s", ti.TICKscript, tick)
	}
	dot := "digraph TASK_ID {\nstream0 -> from1;\n}"
	if ti.Dot != dot {
		t.Fatalf("unexpected dot\ngot\n%s\nexp\n%s\n", ti.Dot, dot)
	}
}

func TestServer_ListTask_Defaults(t *testing.T) {
	s, cli := OpenDefaultServer()
	baseURL := s.URL()
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	id := "task_id"
	tick := "stream\n    |from()\n"
	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       client.StreamTask,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(baseURL + "/tasks")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if exp, got := http.StatusOK, resp.StatusCode; exp != got {
		t.Errorf("unexpected status code, got %d exp %d", got, exp)
	}
	// Response type
	type response struct {
		Tasks []client.Task `json:"tasks"`
	}
	dec := json.NewDecoder(resp.Body)
	tasks := response{}
	dec.Decode(&tasks)
	if exp, got := 1, len(tasks.Tasks); exp != got {
		t.Fatalf("unexpected tasks count, got %d exp %d", got, exp)
	}

	task = tasks.Tasks[0]
	if task.ID != id {
		t.Fatalf("unexpected id got %s exp %s", task.ID, id)
	}
	if task.Type != client.StreamTask {
		t.Fatalf("unexpected type got %v exp %v", task.Type, client.StreamTask)
	}
	if task.Status != client.Disabled {
		t.Fatalf("unexpected status got %v exp %v", task.Status, client.Disabled)
	}
	if !reflect.DeepEqual(task.DBRPs, dbrps) {
		t.Fatalf("unexpected dbrps got %s exp %s", task.DBRPs, dbrps)
	}
	if task.TICKscript != tick {
		t.Fatalf("unexpected TICKscript got %s exp %s", task.TICKscript, tick)
	}
	dot := "digraph task_id {\nstream0 -> from1;\n}"
	if task.Dot != dot {
		t.Fatalf("unexpected dot\ngot\n%s\nexp\n%s\n", task.Dot, dot)
	}
}

func TestServer_CreateTask_ValidIDs(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	testCases := []struct {
		id    string
		valid bool
	}{
		{
			id:    "task_id",
			valid: true,
		},
		{
			id:    "task_id7",
			valid: true,
		},
		{
			id:    "task.id7",
			valid: true,
		},
		{
			id:    "task-id7",
			valid: true,
		},
		{
			id:    "tásk7",
			valid: true,
		},
		{
			id:    "invalid id",
			valid: false,
		},
		{
			id:    "invalid*id",
			valid: false,
		},
		{
			id:    "task/id7",
			valid: false,
		},
	}

	for _, tc := range testCases {
		id := tc.id
		ttype := client.StreamTask
		dbrps := []client.DBRP{
			{
				Database:        "mydb",
				RetentionPolicy: "myrp",
			},
		}
		tick := `stream
    |from()
        .measurement('test')
`
		task, err := cli.CreateTask(client.CreateTaskOptions{
			ID:         id,
			Type:       ttype,
			DBRPs:      dbrps,
			TICKscript: tick,
			Status:     client.Disabled,
		})
		if !tc.valid {
			exp := fmt.Sprintf("task ID must contain only letters, numbers, '-', '.' and '_'. %q", id)
			if err.Error() != exp {
				t.Errorf("unexpected error: got %s exp %s", err.Error(), exp)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}

		ti, err := cli.Task(task.Link, nil)
		if err != nil {
			t.Fatal(err)
		}

		if ti.Error != "" {
			t.Fatal(ti.Error)
		}
		if ti.ID != id {
			t.Fatalf("unexpected id got %s exp %s", ti.ID, id)
		}
		if ti.Type != client.StreamTask {
			t.Fatalf("unexpected type got %v exp %v", ti.Type, client.StreamTask)
		}
		if ti.Status != client.Disabled {
			t.Fatalf("unexpected status got %v exp %v", ti.Status, client.Disabled)
		}
		if !reflect.DeepEqual(ti.DBRPs, dbrps) {
			t.Fatalf("unexpected dbrps got %s exp %s", ti.DBRPs, dbrps)
		}
		if ti.TICKscript != tick {
			t.Fatalf("unexpected TICKscript got %s exp %s", ti.TICKscript, tick)
		}
		dot := "digraph " + id + " {\nstream0 -> from1;\n}"
		if ti.Dot != dot {
			t.Fatalf("unexpected dot\ngot\n%s\nexp\n%s\n", ti.Dot, dot)
		}
	}
}

func TestServer_CreateRecording_ValidIDs(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()
	ttype := client.StreamTask
	dbrps := []client.DBRP{
		{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		},
	}
	tick := `stream
    |from()
        .measurement('test')
`
	_, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         "task_id",
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		id    string
		valid bool
	}{
		{
			id:    "recording_id",
			valid: true,
		},
		{
			id:    "recording_id7",
			valid: true,
		},
		{
			id:    "recording.id7",
			valid: true,
		},
		{
			id:    "recording-id7",
			valid: true,
		},
		{
			id:    "récording7",
			valid: true,
		},
		{
			id:    "invalid id",
			valid: false,
		},
		{
			id:    "invalid*id",
			valid: false,
		},
		{
			id:    "recording/id7",
			valid: false,
		},
	}

	for _, tc := range testCases {
		id := tc.id
		recording, err := cli.RecordStream(client.RecordStreamOptions{
			ID:   id,
			Task: "task_id",
			Stop: time.Date(1970, 1, 1, 0, 0, 10, 0, time.UTC),
		})
		if !tc.valid {
			exp := fmt.Sprintf("recording ID must contain only letters, numbers, '-', '.' and '_'. %q", id)
			if err.Error() != exp {
				t.Errorf("unexpected error: got %s exp %s", err.Error(), exp)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}

		recording, err = cli.Recording(recording.Link)
		if err != nil {
			t.Fatal(err)
		}

		if exp, got := id, recording.ID; got != exp {
			t.Errorf("unexpected recording ID got %s exp %s", got, exp)
		}
	}
}

func TestServer_CreateReplay_ValidIDs(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()
	ttype := client.StreamTask
	dbrps := []client.DBRP{
		{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		},
	}
	tick := `stream
    |from()
        .measurement('test')
`

	_, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         "task_id",
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = cli.RecordStream(client.RecordStreamOptions{
		ID:   "recording_id",
		Task: "task_id",
		Stop: time.Date(1970, 1, 1, 0, 0, 10, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		id    string
		valid bool
	}{
		{
			id:    "replay_id",
			valid: true,
		},
		{
			id:    "replay_id7",
			valid: true,
		},
		{
			id:    "replay.id7",
			valid: true,
		},
		{
			id:    "replay-id7",
			valid: true,
		},
		{
			id:    "réplay7",
			valid: true,
		},
		{
			id:    "invalid id",
			valid: false,
		},
		{
			id:    "invalid*id",
			valid: false,
		},
		{
			id:    "replay/id7",
			valid: false,
		},
	}

	for _, tc := range testCases {
		id := tc.id
		replay, err := cli.CreateReplay(client.CreateReplayOptions{
			ID:            id,
			Task:          "task_id",
			Recording:     "recording_id",
			Clock:         client.Fast,
			RecordingTime: true,
		})
		if !tc.valid {
			exp := fmt.Sprintf("replay ID must contain only letters, numbers, '-', '.' and '_'. %q", id)
			if err.Error() != exp {
				t.Errorf("unexpected error: got %s exp %s", err.Error(), exp)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}

		replay, err = cli.Replay(replay.Link)
		if err != nil {
			t.Fatal(err)
		}

		if exp, got := id, replay.ID; got != exp {
			t.Errorf("unexpected replay ID got %s exp %s", got, exp)
		}
	}
}
