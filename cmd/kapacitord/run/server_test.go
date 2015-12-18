package run_test

import (
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/kapacitor"
)

func TestServer_Ping(t *testing.T) {
	s := OpenDefaultServer()
	defer s.Close()
	r, err := s.HTTPGet(s.URL() + "/api/v1/ping")
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
	resp, err := http.Get(s.URL() + "/api/v1/ping")
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

	endpoint := fmt.Sprintf("%s/api/v1/%s/count", s.URL(), name)

	// Request data before any writes and expect null responses
	nullResponse := `{"Series":null,"Err":null}`
	err = s.HTTPGetRetry(endpoint, "", nullResponse, 1000, time.Millisecond*5)
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
	err = s.HTTPGetRetry(endpoint, nullResponse, exp, 100, time.Millisecond*5)
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

	endpoint := fmt.Sprintf("%s/api/v1/%s/count", s.URL(), name)

	nullResponse := `{"Series":null,"Err":null}`
	exp := `{"Series":[{"name":"cpu","columns":["time","count"],"values":[["1971-01-01T00:00:01.002Z",2]]}],"Err":null}`
	err = s.HTTPGetRetry(endpoint, nullResponse, exp, 100, time.Millisecond*5)
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
