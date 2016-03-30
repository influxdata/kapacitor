package client_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/client/v1"
)

func newClient(handler http.Handler) (*httptest.Server, *client.Client, error) {
	ts := httptest.NewServer(handler)
	config := client.Config{
		URL: ts.URL,
	}
	cli, err := client.New(config)
	return ts, cli, err
}
func Test_NewClient_Error(t *testing.T) {
	_, err := client.New(client.Config{
		URL: "udp://badurl",
	})
	if err == nil {
		t.Error("expected error from client.New")
	}
}

func Test_ReportsErrors(t *testing.T) {

	testCases := []struct {
		name string
		fnc  func(c *client.Client) error
	}{
		{
			name: "Ping",
			fnc: func(c *client.Client) error {
				_, _, err := c.Ping()
				return err
			},
		},
		{
			name: "ListTasks",
			fnc: func(c *client.Client) error {
				_, err := c.ListTasks(nil)
				return err
			},
		},
		{
			name: "Task",
			fnc: func(c *client.Client) error {
				_, err := c.Task("", false)
				return err
			},
		},
		{
			name: "ListRecordings",
			fnc: func(c *client.Client) error {
				_, err := c.ListRecordings(nil)
				return err
			},
		},
		{
			name: "RecordStream",
			fnc: func(c *client.Client) error {
				_, err := c.RecordStream("", 0)
				return err
			},
		},
		{
			name: "RecordBatch",
			fnc: func(c *client.Client) error {
				_, err := c.RecordBatch("", "", time.Time{}, time.Time{}, 0)
				return err
			},
		},
		{
			name: "RecordQuery",
			fnc: func(c *client.Client) error {
				_, err := c.RecordQuery("", "", "")
				return err
			},
		},
		{
			name: "Recording",
			fnc: func(c *client.Client) error {
				_, err := c.Recording("")
				return err
			},
		},
		{
			name: "Replay",
			fnc: func(c *client.Client) error {
				err := c.Replay("", "", false, false)
				return err
			},
		},
		{
			name: "Define",
			fnc: func(c *client.Client) error {
				err := c.Define("", "", nil, nil, false)
				return err
			},
		},
		{
			name: "Enable",
			fnc: func(c *client.Client) error {
				err := c.Enable("")
				return err
			},
		},
		{
			name: "Disable",
			fnc: func(c *client.Client) error {
				err := c.Disable("")
				return err
			},
		},
		{
			name: "Reload",
			fnc: func(c *client.Client) error {
				err := c.Reload("")
				return err
			},
		},
		{
			name: "DeleteTask",
			fnc: func(c *client.Client) error {
				err := c.DeleteTask("")
				return err
			},
		},
		{
			name: "DeleteRecording",
			fnc: func(c *client.Client) error {
				err := c.DeleteRecording("")
				return err
			},
		},
		{
			name: "LogLevel",
			fnc: func(c *client.Client) error {
				err := c.LogLevel("")
				return err
			},
		},
	}
	for _, tc := range testCases {
		s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		err = tc.fnc(c)
		if err == nil {
			t.Fatalf("expected error from call to %s", tc.name)
		}

		s, c, err = newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, `{"Error":"custom error message"}`)
		}))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		err = tc.fnc(c)
		if err == nil {
			t.Fatalf("expected error from call to %s", tc.name)
		}
		if exp, got := "custom error message", err.Error(); exp != got {
			t.Errorf("unexpected error message: got: %s exp: %s", got, exp)
		}
	}
}

func Test_PingVersion(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ping" && r.Method == "GET" {
			w.Header().Set("X-Kapacitor-Version", "versionStr")
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, version, err := c.Ping()
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "versionStr", version; exp != got {
		t.Errorf("unexpected version: got: %s exp: %s", got, exp)
	}
}

func Test_ListTasks(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/tasks" && r.Method == "GET" && r.URL.Query().Get("tasks") == "t1,t2" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
"Tasks":[
	{
		"Name":"t1",
		"Type":"stream",
		"DBRPs":[{"db":"db","rp":"rp"}],
		"Enabled" : false,
		"Executing" : false
	},
	{
		"Name":"t2",
		"Type":"batch",
		"DBRPs":[{"db":"db","rp":"rp"}],
		"Enabled" : true,
		"Executing" : true,
		"ExecutionStats": {
			"TaskStats" : {
				"throughput" : 5.6
			},
			"NodeStats" : {
				"stream1" : {
					"processed" : 1500,
					"avg_exec_time_ns": 2345.83
				}
			}
		}
	}
]}`)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	tasks, err := c.ListTasks([]string{"t1", "t2"})
	if err != nil {
		t.Fatal(err)
	}
	exp := []client.TaskSummary{
		{
			Name: "t1",
			Type: "stream",
			DBRPs: []client.DBRP{{
				Database:        "db",
				RetentionPolicy: "rp",
			}},
			Enabled:        false,
			Executing:      false,
			ExecutionStats: client.ExecutionStats{},
		},
		{
			Name: "t2",
			Type: "batch",
			DBRPs: []client.DBRP{{
				Database:        "db",
				RetentionPolicy: "rp",
			}},
			Enabled:   true,
			Executing: true,
			ExecutionStats: client.ExecutionStats{
				TaskStats: map[string]float64{
					"throughput": 5.6,
				},
				NodeStats: map[string]map[string]float64{
					"stream1": map[string]float64{
						"processed":        1500.0,
						"avg_exec_time_ns": 2345.83,
					},
				},
			},
		},
	}
	if !reflect.DeepEqual(exp, tasks) {
		t.Errorf("unexpected task list: got:\n%v\nexp:\n%v", tasks, exp)
	}
}

func Test_Task(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/task" && r.Method == "GET" && r.URL.Query().Get("name") == "t1" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
	"Name":"t1",
	"Type":"stream",
	"DBRPs":[{"db":"db","rp":"rp"}],
	"TICKscript":"stream\n    |from()\n        .measurement('cpu')\n",
	"Dot": "digraph t1 {\n}",
	"Enabled" : true,
	"Executing" : false,
	"Error": ""
}`)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	task, err := c.Task("t1", false)
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Task{
		Name: "t1",
		Type: "stream",
		DBRPs: []client.DBRP{{
			Database:        "db",
			RetentionPolicy: "rp",
		}},
		TICKscript: `stream
    |from()
        .measurement('cpu')
`,
		Dot:            "digraph t1 {\n}",
		Enabled:        true,
		Executing:      false,
		Error:          "",
		ExecutionStats: client.ExecutionStats{},
	}
	if !reflect.DeepEqual(exp, task) {
		t.Errorf("unexpected task:\ngot:\n%v\nexp:\n%v", task, exp)
	}
}

func Test_Task_Labels(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/task" && r.Method == "GET" &&
			r.URL.Query().Get("name") == "t1" &&
			r.URL.Query().Get("labels") == "true" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
	"Name":"t1",
	"Type":"stream",
	"DBRPs":[{"db":"db","rp":"rp"}],
	"TICKscript":"stream\n    |from()\n        .measurement('cpu')\n",
	"Dot": "digraph t1 {\n}",
	"Enabled" : true,
	"Executing" : false,
	"Error": ""
}`)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	task, err := c.Task("t1", true)
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Task{
		Name: "t1",
		Type: "stream",
		DBRPs: []client.DBRP{{
			Database:        "db",
			RetentionPolicy: "rp",
		}},
		TICKscript: `stream
    |from()
        .measurement('cpu')
`,
		Dot:            "digraph t1 {\n}",
		Enabled:        true,
		Executing:      false,
		Error:          "",
		ExecutionStats: client.ExecutionStats{},
	}
	if !reflect.DeepEqual(exp, task) {
		t.Errorf("unexpected task:\ngot:\n%v\nexp:\n%v", task, exp)
	}
}

func Test_ListRecordings(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/recordings" && r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
"Recordings":[
	{
		"ID":"rid1",
		"Type":"batch",
		"Size": 42,
		"Created" : "2016-03-31T11:24:55.526388889Z",
		"Error": ""
	},
	{
		"ID":"rid2",
		"Type":"stream",
		"Size": 4200,
		"Created" : "2016-03-31T10:24:55.526388889Z",
		"Error": ""
	}
]}`)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	tasks, err := c.ListRecordings([]string{"rid1", "rid2"})
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Recordings{
		{
			ID:      "rid2",
			Type:    "stream",
			Size:    4200,
			Created: time.Date(2016, 3, 31, 10, 24, 55, 526388889, time.UTC),
		},
		{
			ID:      "rid1",
			Type:    "batch",
			Size:    42,
			Created: time.Date(2016, 3, 31, 11, 24, 55, 526388889, time.UTC),
		},
	}
	if !reflect.DeepEqual(exp, tasks) {
		t.Errorf("unexpected recording list:\ngot:\n%v\nexp:\n%v", tasks, exp)
	}
}

func Test_Record(t *testing.T) {
	testCases := []struct {
		name         string
		fnc          func(c *client.Client) (string, error)
		checkRequest func(r *http.Request) bool
	}{
		{
			name: "RecordStream",
			fnc: func(c *client.Client) (string, error) {
				return c.RecordStream("taskname", time.Minute)
			},
			checkRequest: func(r *http.Request) bool {
				return r.URL.Query().Get("type") == "stream" &&
					r.URL.Query().Get("name") == "taskname" &&
					r.URL.Query().Get("duration") == "1m0s"
			},
		},
		{
			name: "RecordBatch",
			fnc: func(c *client.Client) (string, error) {
				return c.RecordBatch(
					"taskname",
					"cluster",
					time.Date(2016, 3, 31, 10, 24, 55, 526388889, time.UTC),
					time.Date(2016, 3, 31, 11, 24, 55, 526388889, time.UTC),
					time.Hour*5,
				)
			},
			checkRequest: func(r *http.Request) bool {
				return r.URL.Query().Get("type") == "batch" &&
					r.URL.Query().Get("name") == "taskname" &&
					r.URL.Query().Get("cluster") == "cluster" &&
					r.URL.Query().Get("start") == "2016-03-31T10:24:55.526388889Z" &&
					r.URL.Query().Get("stop") == "2016-03-31T11:24:55.526388889Z" &&
					r.URL.Query().Get("past") == "5h0m0s"
			},
		},
		{
			name: "RecordQuery",
			fnc: func(c *client.Client) (string, error) {
				return c.RecordQuery("queryStr", "stream", "cluster")
			},
			checkRequest: func(r *http.Request) bool {
				return r.URL.Query().Get("type") == "query" &&
					r.URL.Query().Get("ttype") == "stream" &&
					r.URL.Query().Get("cluster") == "cluster"
			},
		},
	}
	for _, tc := range testCases {
		s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/record" && r.Method == "POST" && tc.checkRequest(r) {
				w.WriteHeader(http.StatusOK)
				fmt.Fprintf(w, `{"RecordingID":"rid1"}`)
			} else {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "request: %v", r)
			}
		}))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		rid, err := tc.fnc(c)
		if err != nil {
			t.Fatal(err)
		}
		if exp, got := "rid1", rid; got != exp {
			t.Errorf("unexpected recording id for test %s: got: %s exp: %s", tc.name, got, exp)
		}
	}
}

func Test_Recording(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/record" && r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
	"ID":"rid1",
	"Type":"batch",
	"Size": 42,
	"Created" : "2016-03-31T11:24:55.526388889Z",
	"Error": ""
}`)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	tasks, err := c.Recording("rid1")
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Recording{
		ID:      "rid1",
		Type:    "batch",
		Size:    42,
		Created: time.Date(2016, 3, 31, 11, 24, 55, 526388889, time.UTC),
	}
	if !reflect.DeepEqual(exp, tasks) {
		t.Errorf("unexpected recording list:\ngot:\n%v\nexp:\n%v", tasks, exp)
	}
}

func Test_Replay(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/replay" && r.Method == "POST" &&
			r.URL.Query().Get("name") == "taskname" &&
			r.URL.Query().Get("id") == "rid1" &&
			r.URL.Query().Get("rec-time") == "false" &&
			r.URL.Query().Get("clock") == "fast" {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = c.Replay("taskname", "rid1", false, true)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_Define(t *testing.T) {
	tickScript := "stream|from().measurement('cpu')"
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		script, _ := ioutil.ReadAll(r.Body)

		if r.URL.Path == "/task" && r.Method == "POST" &&
			r.URL.Query().Get("name") == "taskname" &&
			r.URL.Query().Get("type") == "stream" &&
			r.URL.Query().Get("dbrps") == `[{"db":"dbname","rp":"rpname"}]` &&
			string(script) == tickScript {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = c.Define(
		"taskname",
		"stream",
		[]client.DBRP{{Database: "dbname", RetentionPolicy: "rpname"}},
		strings.NewReader(tickScript),
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
}
func Test_Define_Reload(t *testing.T) {
	requestCount := 0
	tickScript := "stream|from().measurement('cpu')"
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		switch requestCount {
		case 1:
			script, _ := ioutil.ReadAll(r.Body)
			if r.URL.Path == "/task" && r.Method == "POST" &&
				r.URL.Query().Get("name") == "taskname" &&
				r.URL.Query().Get("type") == "stream" &&
				r.URL.Query().Get("dbrps") == `[{"db":"dbname","rp":"rpname"}]` &&
				string(script) == tickScript {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		case 2:
			if r.URL.Path == "/tasks" && r.Method == "GET" &&
				r.URL.Query().Get("tasks") == "taskname" {
				w.WriteHeader(http.StatusOK)
				fmt.Fprintf(w, `{
"Tasks":[
	{
		"Name":"taskname",
		"Type":"stream",
		"DBRPs":[{"db":"db","rp":"rp"}],
		"Enabled": true,
		"Executing": true
	}
]}`)
				return
			}
		case 3:
			if r.URL.Path == "/disable" && r.Method == "POST" {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		case 4:
			if r.URL.Path == "/enable" && r.Method == "POST" {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "request: %v", r)
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = c.Define(
		"taskname",
		"stream",
		[]client.DBRP{{Database: "dbname", RetentionPolicy: "rpname"}},
		strings.NewReader(tickScript),
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := 4, requestCount; got != exp {
		t.Errorf("unexpected request count: got %d exp %d", got, exp)
	}
}

func Test_Enable(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/enable" && r.Method == "POST" &&
			r.URL.Query().Get("name") == "taskname" {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = c.Enable("taskname")
	if err != nil {
		t.Fatal(err)
	}
}

func Test_Disable(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/disable" && r.Method == "POST" &&
			r.URL.Query().Get("name") == "taskname" {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = c.Disable("taskname")
	if err != nil {
		t.Fatal(err)
	}
}

func Test_Reload(t *testing.T) {
	requestCount := 0
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		switch requestCount {
		case 1:
			if r.URL.Path == "/disable" && r.Method == "POST" &&
				r.URL.Query().Get("name") == "taskname" {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		case 2:
			if r.URL.Path == "/enable" && r.Method == "POST" &&
				r.URL.Query().Get("name") == "taskname" {
				w.WriteHeader(http.StatusNoContent)
				return
			}

		}
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "request: %v", r)
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = c.Reload("taskname")
	if err != nil {
		t.Fatal(err)
	}
}

func Test_Reload_SkipEnable(t *testing.T) {
	requestCount := 0
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = c.Reload("taskname")
	if err == nil {
		t.Error("expected error from Reload")
	}
	if exp, got := 1, requestCount; got != exp {
		t.Errorf("unexpected request count: got %d exp %d", got, exp)
	}
}

func Test_DeleteTask(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/task" && r.Method == "DELETE" &&
			r.URL.Query().Get("name") == "taskname" {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = c.DeleteTask("taskname")
	if err != nil {
		t.Fatal(err)
	}
}

func Test_DeleteRecording(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/recording" && r.Method == "DELETE" &&
			r.URL.Query().Get("rid") == "rid1" {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = c.DeleteRecording("rid1")
	if err != nil {
		t.Fatal(err)
	}
}

func Test_LogLevel(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/loglevel" && r.Method == "POST" &&
			r.URL.Query().Get("level") == "DEBUG" {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = c.LogLevel("DEBUG")
	if err != nil {
		t.Fatal(err)
	}
}
