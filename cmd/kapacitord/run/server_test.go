package run_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/cmd/kapacitord/run"
	"github.com/influxdata/kapacitor/services/udf"
	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/toml"
)

func TestServer_Ping(t *testing.T) {
	s := OpenDefaultServer()
	defer s.Close()
	r, err := s.HTTPGet(s.URL() + "/ping")
	if err != nil {
		t.Fatal(err)
	}
	if r != "" {
		t.Fatal("unexpected result")
	}
}

func TestServer_Version(t *testing.T) {
	s := OpenDefaultServer()
	defer s.Close()
	resp, err := http.Get(s.URL() + "/ping")
	if err != nil {
		t.Fatal(err)
	}
	version := resp.Header.Get("X-KAPACITOR-Version")

	if version != "testServer" {
		t.Fatal("unexpected version", version)
	}
}

func TestServer_DefineTask(t *testing.T) {
	s := OpenDefaultServer()
	defer s.Close()

	name := "testTaskName"
	ttype := "stream"
	dbrps := []kapacitor.DBRP{
		{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		},
		{
			Database:        "otherdb",
			RetentionPolicy: "default",
		},
	}
	tick := "stream.from().measurement('test')"
	r, err := s.DefineTask(name, ttype, tick, dbrps)
	if err != nil {
		t.Fatal(err)
	}
	if r != "" {
		t.Fatal("unexpected result", r)
	}

	ti, err := s.GetTask(name)
	if err != nil {
		t.Fatal(err)
	}

	if ti.Error != "" {
		t.Fatal(ti.Error)
	}
	if ti.Name != name {
		t.Fatalf("unexpected name got %s exp %s", ti.Name, name)
	}
	if ti.Type != kapacitor.StreamTask {
		t.Fatalf("unexpected type got %s exp %s", ti.Type, kapacitor.StreamTask)
	}
	if ti.Enabled != false {
		t.Fatalf("unexpected enabled got %v exp %v", ti.Enabled, false)
	}
	if !reflect.DeepEqual(ti.DBRPs, dbrps) {
		t.Fatalf("unexpected dbrps got %s exp %s", ti.DBRPs, dbrps)
	}
	if ti.TICKscript != tick {
		t.Fatalf("unexpected TICKscript got %s exp %s", ti.TICKscript, tick)
	}
	dot := "digraph testTaskName {\nstream0 -> stream1;\n}"
	if ti.Dot != dot {
		t.Fatalf("unexpected dot got %s exp %s", ti.Dot, dot)
	}
}

func TestServer_EnableTask(t *testing.T) {
	s := OpenDefaultServer()
	defer s.Close()

	name := "testTaskName"
	ttype := "stream"
	dbrps := []kapacitor.DBRP{
		{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		},
		{
			Database:        "otherdb",
			RetentionPolicy: "default",
		},
	}
	tick := "stream.from().measurement('test')"
	r, err := s.DefineTask(name, ttype, tick, dbrps)
	if err != nil {
		t.Fatal(err)
	}
	if r != "" {
		t.Fatal("unexpected result", r)
	}

	r, err = s.EnableTask(name)
	if err != nil {
		t.Fatal(err)
	}
	if r != "" {
		t.Fatal("unexpected result", r)
	}

	ti, err := s.GetTask(name)
	if err != nil {
		t.Fatal(err)
	}

	if ti.Error != "" {
		t.Fatal(ti.Error)
	}
	if ti.Name != name {
		t.Fatalf("unexpected name got %s exp %s", ti.Name, name)
	}
	if ti.Type != kapacitor.StreamTask {
		t.Fatalf("unexpected type got %s exp %s", ti.Type, kapacitor.StreamTask)
	}
	if ti.Enabled != true {
		t.Fatalf("unexpected enabled got %v exp %v", ti.Enabled, true)
	}
	if !reflect.DeepEqual(ti.DBRPs, dbrps) {
		t.Fatalf("unexpected dbrps got %s exp %s", ti.DBRPs, dbrps)
	}
	if ti.TICKscript != tick {
		t.Fatalf("unexpected TICKscript got %s exp %s", ti.TICKscript, tick)
	}
	dot := "digraph testTaskName {\nstream0 -> stream1 [label=\"0\"];\n}"
	if ti.Dot != dot {
		t.Fatalf("unexpected dot got %s exp %s", ti.Dot, dot)
	}
}

func TestServer_DisableTask(t *testing.T) {
	s := OpenDefaultServer()
	defer s.Close()

	name := "testTaskName"
	ttype := "stream"
	dbrps := []kapacitor.DBRP{
		{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		},
		{
			Database:        "otherdb",
			RetentionPolicy: "default",
		},
	}
	tick := "stream.from().measurement('test')"
	r, err := s.DefineTask(name, ttype, tick, dbrps)
	if err != nil {
		t.Fatal(err)
	}
	if r != "" {
		t.Fatal("unexpected result", r)
	}

	r, err = s.EnableTask(name)
	if err != nil {
		t.Fatal(err)
	}
	if r != "" {
		t.Fatal("unexpected result", r)
	}

	r, err = s.DisableTask(name)
	if err != nil {
		t.Fatal(err)
	}
	if r != "" {
		t.Fatal("unexpected result", r)
	}

	ti, err := s.GetTask(name)
	if err != nil {
		t.Fatal(err)
	}

	if ti.Error != "" {
		t.Fatal(ti.Error)
	}
	if ti.Name != name {
		t.Fatalf("unexpected name got %s exp %s", ti.Name, name)
	}
	if ti.Type != kapacitor.StreamTask {
		t.Fatalf("unexpected type got %s exp %s", ti.Type, kapacitor.StreamTask)
	}
	if ti.Enabled != false {
		t.Fatalf("unexpected enabled got %v exp %v", ti.Enabled, false)
	}
	if !reflect.DeepEqual(ti.DBRPs, dbrps) {
		t.Fatalf("unexpected dbrps got %s exp %s", ti.DBRPs, dbrps)
	}
	if ti.TICKscript != tick {
		t.Fatalf("unexpected TICKscript got %s exp %s", ti.TICKscript, tick)
	}
	dot := "digraph testTaskName {\nstream0 -> stream1;\n}"
	if ti.Dot != dot {
		t.Fatalf("unexpected dot got %s exp %s", ti.Dot, dot)
	}
}

func TestServer_DeleteTask(t *testing.T) {
	s := OpenDefaultServer()
	defer s.Close()

	name := "testTaskName"
	ttype := "stream"
	dbrps := []kapacitor.DBRP{
		{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		},
		{
			Database:        "otherdb",
			RetentionPolicy: "default",
		},
	}
	tick := "stream.from().measurement('test')"
	r, err := s.DefineTask(name, ttype, tick, dbrps)
	if err != nil {
		t.Fatal(err)
	}
	if r != "" {
		t.Fatal("unexpected result", r)
	}

	err = s.DeleteTask(name)
	if err != nil {
		t.Fatal(err)
	}

	ti, err := s.GetTask(name)
	if err == nil {
		t.Fatal("unexpected task:", ti)
	}
}

func TestServer_StreamTask(t *testing.T) {
	s := OpenDefaultServer()
	defer s.Close()

	name := "testStreamTask"
	ttype := "stream"
	dbrps := []kapacitor.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	tick := `
stream
	.from().measurement('test')
	.window()
		.period(10s)
		.every(10s)
	.mapReduce(influxql.count('value'))
	.httpOut('count')
`

	r, err := s.DefineTask(name, ttype, tick, dbrps)
	if err != nil {
		t.Fatal(err)
	}
	if r != "" {
		t.Fatal("unexpected result", r)
	}

	r, err = s.EnableTask(name)
	if err != nil {
		t.Fatal(err)
	}
	if r != "" {
		t.Fatal("unexpected result", r)
	}

	endpoint := fmt.Sprintf("%s/%s/count", s.URL(), name)

	// Request data before any writes and expect null responses
	nullResponse := `{"Series":null,"Err":null}`
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

	exp := `{"Series":[{"name":"test","columns":["time","count"],"values":[["1970-01-01T00:00:10Z",15]]}],"Err":null}`
	err = s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5)
	if err != nil {
		t.Error(err)
	}
}

func TestServer_BatchTask(t *testing.T) {
	c := NewConfig()
	c.InfluxDB.Enabled = true
	count := 0
	db := NewInfluxDB(func(q string) *client.Response {
		if len(q) > 6 && q[:6] == "SELECT" {
			count++
			return &client.Response{
				Results: []client.Result{{
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
	c.InfluxDB.URLs = []string{db.URL()}
	s := OpenServer(c)
	defer s.Close()

	name := "testBatchTask"
	ttype := "batch"
	dbrps := []kapacitor.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	tick := `
batch
	.query(' SELECT value from mydb.myrp.cpu ')
		.period(5ms)
		.every(5ms)
	.mapReduce(influxql.count('value'))
	.httpOut('count')
`

	r, err := s.DefineTask(name, ttype, tick, dbrps)
	if err != nil {
		t.Fatal(err)
	}
	if r != "" {
		t.Fatal("unexpected result", r)
	}

	r, err = s.EnableTask(name)
	if err != nil {
		t.Fatal(err)
	}
	if r != "" {
		t.Fatal("unexpected result", r)
	}

	endpoint := fmt.Sprintf("%s/%s/count", s.URL(), name)

	exp := `{"Series":[{"name":"cpu","columns":["time","count"],"values":[["1971-01-01T00:00:01.002Z",2]]}],"Err":null}`
	err = s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5)
	if err != nil {
		t.Error(err)
	}
	r, err = s.DisableTask(name)
	if err != nil {
		t.Fatal(err)
	}
	if r != "" {
		t.Fatal("unexpected result", r)
	}

	if count == 0 {
		t.Error("unexpected query count", count)
	}
}

func TestServer_RecordReplayStream(t *testing.T) {
	s := OpenDefaultServer()
	defer s.Close()

	name := "testStreamTask"
	ttype := "stream"
	dbrps := []kapacitor.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}

	tmpDir, err := ioutil.TempDir("", "testStreamTaskRecording")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	tick := `
stream
	.from().measurement('test')
	.window()
		.period(10s)
		.every(10s)
	.mapReduce(influxql.count('value'))
	.alert()
		.id('test-count')
		.message('{{ .ID }} got: {{ index .Fields "count" }}')
		.crit(lambda: TRUE)
		.log('` + tmpDir + `/alert.log')
`

	r, err := s.DefineTask(name, ttype, tick, dbrps)
	if err != nil {
		t.Fatal(err)
	}
	if r != "" {
		t.Fatal("unexpected result", r)
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
	rid := make(chan string, 1)
	started := make(chan struct{})
	go func() {
		id, err := s.DoStreamRecording(name, 10*time.Second, started)
		if err != nil {
			t.Fatal(err)
		}
		rid <- id
	}()
	<-started
	v := url.Values{}
	v.Add("precision", "s")
	s.MustWrite("mydb", "myrp", points, v)
	id := <-rid

	_, err = s.DoReplay(name, id)
	if err != nil {
		t.Fatal(err)
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
}

func TestServer_RecordReplayBatch(t *testing.T) {
	c := NewConfig()
	c.InfluxDB.Enabled = true
	value := 0
	db := NewInfluxDB(func(q string) *client.Response {
		if len(q) > 6 && q[:6] == "SELECT" {
			r := &client.Response{
				Results: []client.Result{{
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
	c.InfluxDB.URLs = []string{db.URL()}
	s := OpenServer(c)
	defer s.Close()

	name := "testBatchTask"
	ttype := "batch"
	dbrps := []kapacitor.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}

	tmpDir, err := ioutil.TempDir("", "testBatchTaskRecording")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	tick := `
batch
	.query('SELECT value from mydb.myrp.cpu')
		.period(2s)
		.every(2s)
	.alert()
		.id('test-batch')
		.message('{{ .ID }} got: {{ index .Fields "value" }}')
		.crit(lambda: "value" > 2.0)
		.log('` + tmpDir + `/alert.log')
`

	r, err := s.DefineTask(name, ttype, tick, dbrps)
	if err != nil {
		t.Fatal(err)
	}
	if r != "" {
		t.Fatal("unexpected result", r)
	}

	id, err := s.DoBatchRecording(name, time.Second*8)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.DoReplay(name, id)
	if err != nil {
		t.Fatal(err)
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
}

func TestServer_UDFAgents(t *testing.T) {
	dir, err := os.Getwd()
	udfDir := filepath.Clean(filepath.Join(dir, "../../../udf"))
	if err != nil {
		t.Fatal(err)
	}

	agents := []struct {
		buildFunc func() error
		config    udf.FunctionConfig
	}{
		// Go
		{
			buildFunc: func() error { return nil },
			config: udf.FunctionConfig{
				Prog:    "go",
				Args:    []string{"run", filepath.Join(udfDir, "agent/examples/moving_avg.go")},
				Timeout: toml.Duration(time.Minute),
			},
		},
		// Python
		{
			buildFunc: func() error { return nil },
			config: udf.FunctionConfig{
				Prog:    "python2",
				Args:    []string{"-u", filepath.Join(udfDir, "agent/examples/moving_avg.py")},
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
		testAgent(t, c)
	}
}

func testAgent(t *testing.T, c *run.Config) {
	s := NewServer(c)
	err := s.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	name := "testUDFTask"
	ttype := "stream"
	dbrps := []kapacitor.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	tick := `
stream
	.from().measurement('test')
	.movingAvg()
		.field('value')
		.size(10)
		.as('mean')
	.window()
		.period(11s)
		.every(11s)
	.mapReduce(influxql.last('mean')).as('mean')
	.httpOut('moving_avg')
`

	r, err := s.DefineTask(name, ttype, tick, dbrps)
	if err != nil {
		t.Fatal(err)
	}
	if r != "" {
		t.Fatal("unexpected result", r)
	}

	r, err = s.EnableTask(name)
	if err != nil {
		t.Fatal(err)
	}
	if r != "" {
		t.Fatal("unexpected result", r)
	}

	endpoint := fmt.Sprintf("%s/%s/moving_avg", s.URL(), name)

	// Request data before any writes and expect null responses
	nullResponse := `{"Series":null,"Err":null}`
	err = s.HTTPGetRetry(endpoint, nullResponse, 100, time.Millisecond*5)
	if err != nil {
		t.Error(err)
	}

	points := `test value=1 0000000000
test value=1 0000000001
test value=1 0000000002
test value=1 0000000003
test value=1 0000000004
test value=1 0000000005
test value=1 0000000006
test value=1 0000000007
test value=1 0000000008
test value=1 0000000009
test value=0 0000000010
test value=0 0000000011
`
	v := url.Values{}
	v.Add("precision", "s")
	s.MustWrite("mydb", "myrp", points, v)

	exp := `{"Series":[{"name":"test","columns":["time","mean"],"values":[["1970-01-01T00:00:11Z",0.9]]}],"Err":null}`
	err = s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5)
	if err != nil {
		t.Error(err)
	}
}
