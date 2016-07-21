package client_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor/client/v1"
)

func newClient(handler http.Handler) (*httptest.Server, *client.Client, error) {
	return newClientWithConfig(handler, client.Config{})
}

func newClientWithConfig(handler http.Handler, config client.Config) (*httptest.Server, *client.Client, error) {
	ts := httptest.NewServer(handler)
	config.URL = ts.URL
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
			name: "CreateTask",
			fnc: func(c *client.Client) error {
				_, err := c.CreateTask(client.CreateTaskOptions{})
				return err
			},
		},
		{
			name: "UpdateTask",
			fnc: func(c *client.Client) error {
				_, err := c.UpdateTask(c.TaskLink(""), client.UpdateTaskOptions{})
				return err
			},
		},
		{
			name: "DeleteTask",
			fnc: func(c *client.Client) error {
				err := c.DeleteTask(c.TaskLink(""))
				return err
			},
		},
		{
			name: "Task",
			fnc: func(c *client.Client) error {
				_, err := c.Task(c.TaskLink(""), nil)
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
			name: "CreateTemplate",
			fnc: func(c *client.Client) error {
				_, err := c.CreateTemplate(client.CreateTemplateOptions{})
				return err
			},
		},
		{
			name: "UpdateTemplate",
			fnc: func(c *client.Client) error {
				_, err := c.UpdateTemplate(c.TemplateLink(""), client.UpdateTemplateOptions{})
				return err
			},
		},
		{
			name: "DeleteTemplate",
			fnc: func(c *client.Client) error {
				err := c.DeleteTemplate(c.TemplateLink(""))
				return err
			},
		},
		{
			name: "Template",
			fnc: func(c *client.Client) error {
				_, err := c.Template(c.TemplateLink(""), nil)
				return err
			},
		},
		{
			name: "ListTemplates",
			fnc: func(c *client.Client) error {
				_, err := c.ListTemplates(nil)
				return err
			},
		},
		{
			name: "RecordStream",
			fnc: func(c *client.Client) error {
				_, err := c.RecordStream(client.RecordStreamOptions{})
				return err
			},
		},
		{
			name: "RecordBatch",
			fnc: func(c *client.Client) error {
				_, err := c.RecordBatch(client.RecordBatchOptions{})
				return err
			},
		},
		{
			name: "RecordQuery",
			fnc: func(c *client.Client) error {
				_, err := c.RecordQuery(client.RecordQueryOptions{Type: client.StreamTask})
				return err
			},
		},
		{
			name: "Recording",
			fnc: func(c *client.Client) error {
				_, err := c.Recording(c.RecordingLink(""))
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
			name: "DeleteRecording",
			fnc: func(c *client.Client) error {
				err := c.DeleteRecording(c.RecordingLink(""))
				return err
			},
		},
		{
			name: "CreateReplay",
			fnc: func(c *client.Client) error {
				_, err := c.CreateReplay(client.CreateReplayOptions{})
				return err
			},
		},
		{
			name: "ReplayBatch",
			fnc: func(c *client.Client) error {
				_, err := c.ReplayBatch(client.ReplayBatchOptions{})
				return err
			},
		},
		{
			name: "ReplayQuery",
			fnc: func(c *client.Client) error {
				_, err := c.ReplayQuery(client.ReplayQueryOptions{})
				return err
			},
		},
		{
			name: "DeleteReplay",
			fnc: func(c *client.Client) error {
				err := c.DeleteReplay(c.ReplayLink(""))
				return err
			},
		},
		{
			name: "Replay",
			fnc: func(c *client.Client) error {
				_, err := c.Replay(c.ReplayLink(""))
				return err
			},
		},
		{
			name: "ListReplay",
			fnc: func(c *client.Client) error {
				_, err := c.ListReplays(nil)
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
		if r.URL.Path == "/kapacitor/v1/ping" && r.Method == "GET" {
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

func Test_Task(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/tasks/t1" && r.Method == "GET" &&
			r.URL.Query().Get("dot-view") == "attributes" &&
			r.URL.Query().Get("script-format") == "formatted" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
	"link": {"rel":"self", "href":"/kapacitor/v1/tasks/t1"},
	"id": "t1",
	"type":"stream",
	"dbrps":[{"db":"db","rp":"rp"}],
	"script":"stream\n    |from()\n        .measurement('cpu')\n",
	"dot": "digraph t1 {}",
	"status" : "enabled",
	"executing" : false,
	"error": ""
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

	task, err := c.Task(c.TaskLink("t1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Task{
		Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/tasks/t1"},
		ID:   "t1",
		Type: client.StreamTask,
		DBRPs: []client.DBRP{{
			Database:        "db",
			RetentionPolicy: "rp",
		}},
		TICKscript: `stream
    |from()
        .measurement('cpu')
`,
		Dot:            "digraph t1 {}",
		Status:         client.Enabled,
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
		if r.URL.Path == "/kapacitor/v1/tasks/t1" && r.Method == "GET" &&
			r.URL.Query().Get("dot-view") == "labels" &&
			r.URL.Query().Get("script-format") == "formatted" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
	"link": {"rel":"self", "href":"/kapacitor/v1/tasks/t1"},
	"id": "t1",
	"type":"stream",
	"dbrps":[{"db":"db","rp":"rp"}],
	"script":"stream\n    |from()\n        .measurement('cpu')\n",
	"dot": "digraph t1 {\n}",
	"status" : "enabled",
	"executing" : false,
	"error": ""
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

	task, err := c.Task(c.TaskLink("t1"), &client.TaskOptions{DotView: "labels"})
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Task{
		Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/tasks/t1"},
		ID:   "t1",
		Type: client.StreamTask,
		DBRPs: []client.DBRP{{
			Database:        "db",
			RetentionPolicy: "rp",
		}},
		TICKscript: `stream
    |from()
        .measurement('cpu')
`,
		Dot:            "digraph t1 {\n}",
		Status:         client.Enabled,
		Executing:      false,
		Error:          "",
		ExecutionStats: client.ExecutionStats{},
	}
	if !reflect.DeepEqual(exp, task) {
		t.Errorf("unexpected task:\ngot:\n%v\nexp:\n%v", task, exp)
	}
}

func Test_Task_RawFormat(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/tasks/t1" && r.Method == "GET" &&
			r.URL.Query().Get("dot-view") == "attributes" &&
			r.URL.Query().Get("script-format") == "raw" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
	"link": {"rel":"self", "href":"/kapacitor/v1/tasks/t1"},
	"id": "t1",
	"type":"stream",
	"dbrps":[{"db":"db","rp":"rp"}],
	"script":"stream|from().measurement('cpu')",
	"dot": "digraph t1 {\n}",
	"status" : "enabled",
	"executing" : false,
	"error": ""
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

	task, err := c.Task(c.TaskLink("t1"), &client.TaskOptions{ScriptFormat: "raw"})
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Task{
		Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/tasks/t1"},
		ID:   "t1",
		Type: client.StreamTask,
		DBRPs: []client.DBRP{{
			Database:        "db",
			RetentionPolicy: "rp",
		}},
		TICKscript:     "stream|from().measurement('cpu')",
		Dot:            "digraph t1 {\n}",
		Status:         client.Enabled,
		Executing:      false,
		Error:          "",
		ExecutionStats: client.ExecutionStats{},
	}
	if !reflect.DeepEqual(exp, task) {
		t.Errorf("unexpected task:\ngot:\n%v\nexp:\n%v", task, exp)
	}
}

func Test_CreateTask(t *testing.T) {
	tickScript := "stream|from().measurement('cpu')"
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var task client.CreateTaskOptions
		body, _ := ioutil.ReadAll(r.Body)
		err := json.Unmarshal(body, &task)
		if err != nil {
			t.Fatal(err)
		}

		if r.URL.Path == "/kapacitor/v1/tasks" && r.Method == "POST" {
			exp := client.CreateTaskOptions{
				ID:         "taskname",
				Type:       client.StreamTask,
				DBRPs:      []client.DBRP{{Database: "dbname", RetentionPolicy: "rpname"}},
				TICKscript: tickScript,
				Status:     client.Disabled,
				Vars: client.Vars{
					"var1": {
						Value: true,
						Type:  client.VarBool,
					},
				},
			}
			if !reflect.DeepEqual(exp, task) {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "unexpected CreateTask body: got:\n%v\nexp:\n%v\n", task, exp)
			} else {
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, `{"link": {"rel":"self", "href":"/kapacitor/v1/tasks/taskname"}, "id":"taskname"}`)
			}
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	task, err := c.CreateTask(client.CreateTaskOptions{
		ID:         "taskname",
		Type:       client.StreamTask,
		DBRPs:      []client.DBRP{{Database: "dbname", RetentionPolicy: "rpname"}},
		TICKscript: tickScript,
		Status:     client.Disabled,
		Vars: client.Vars{
			"var1": {
				Value: true,
				Type:  client.VarBool,
			},
		},
	})
	if got, exp := string(task.Link.Href), "/kapacitor/v1/tasks/taskname"; got != exp {
		t.Errorf("unexpected task link got %s exp %s", got, exp)
	}
	if got, exp := task.ID, "taskname"; got != exp {
		t.Errorf("unexpected task ID got %s exp %s", got, exp)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func Test_UpdateTask(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var task client.UpdateTaskOptions
		task.Status = client.Enabled
		body, _ := ioutil.ReadAll(r.Body)
		err := json.Unmarshal(body, &task)
		if err != nil {
			t.Fatal(err)
		}

		if r.URL.Path == "/kapacitor/v1/tasks/taskname" && r.Method == "PATCH" {
			exp := client.UpdateTaskOptions{
				DBRPs:  []client.DBRP{{Database: "newdb", RetentionPolicy: "rpname"}},
				Status: client.Enabled,
				Vars: client.Vars{
					"var1": {
						Value: int64(42),
						Type:  client.VarInt,
					},
					"var2": {
						Value: float64(42),
						Type:  client.VarFloat,
					},
				},
			}
			if !reflect.DeepEqual(exp, task) {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "unexpected UpdateTask body: got:\n%v\nexp:\n%v\n", task, exp)
			} else {
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, `{"link": {"rel":"self", "href":"/kapacitor/v1/tasks/taskname"}, "id":"taskname"}`)
			}
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	task, err := c.UpdateTask(
		c.TaskLink("taskname"),
		client.UpdateTaskOptions{
			DBRPs: []client.DBRP{{Database: "newdb", RetentionPolicy: "rpname"}},
			Vars: client.Vars{
				"var1": {
					Value: int64(42),
					Type:  client.VarInt,
				},
				"var2": {
					Value: float64(42),
					Type:  client.VarFloat,
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, exp := task.Link.Href, "/kapacitor/v1/tasks/taskname"; got != exp {
		t.Errorf("unexpected link.Href got %s exp %s", got, exp)
	}
}

func Test_UpdateTask_Enable(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var task client.UpdateTaskOptions
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body, &task)

		if r.URL.Path == "/kapacitor/v1/tasks/taskname" && r.Method == "PATCH" {
			exp := client.UpdateTaskOptions{
				Status: client.Enabled,
			}
			if !reflect.DeepEqual(exp, task) {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "unexpected UpdateTask body: got:\n%v\nexp:\n%v\n", task, exp)
			} else {
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, `{"link": {"rel":"self", "href":"/kapacitor/v1/tasks/taskname"}, "id":"taskname", "status": "enabled"}`)
			}
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	task, err := c.UpdateTask(
		c.TaskLink("taskname"),
		client.UpdateTaskOptions{
			Status: client.Enabled,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, exp := task.Link.Href, "/kapacitor/v1/tasks/taskname"; got != exp {
		t.Errorf("unexpected link.Href got %s exp %s", got, exp)
	}
	if got, exp := task.Status, client.Enabled; got != exp {
		t.Errorf("unexpected task.Status got %s exp %s", got, exp)
	}
}

func Test_UpdateTask_Disable(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var task client.UpdateTaskOptions
		task.Status = client.Enabled
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body, &task)

		if r.URL.Path == "/kapacitor/v1/tasks/taskname" && r.Method == "PATCH" {
			exp := client.UpdateTaskOptions{
				Status: client.Disabled,
			}
			if !reflect.DeepEqual(exp, task) {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "unexpected UpdateTask body: got:\n%v\nexp:\n%v\n", task, exp)
			} else {
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, `{"link": {"rel":"self", "href":"/kapacitor/v1/tasks/taskname"}, "id":"taskname", "status": "disabled"}`)
			}
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	task, err := c.UpdateTask(
		c.TaskLink("taskname"),
		client.UpdateTaskOptions{
			Status: client.Disabled,
		})
	if err != nil {
		t.Fatal(err)
	}
	if got, exp := task.Link.Href, "/kapacitor/v1/tasks/taskname"; got != exp {
		t.Errorf("unexpected link.Href got %s exp %s", got, exp)
	}
	if got, exp := task.Status, client.Disabled; got != exp {
		t.Errorf("unexpected task.Status got %s exp %s", got, exp)
	}
}

func Test_DeleteTask(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/tasks/taskname" && r.Method == "DELETE" {
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

	err = c.DeleteTask(c.TaskLink("taskname"))
	if err != nil {
		t.Fatal(err)
	}
}

func Test_ListTasks(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/tasks" && r.Method == "GET" &&
			r.URL.Query().Get("pattern") == "" &&
			r.URL.Query().Get("fields") == "" &&
			r.URL.Query().Get("dot-view") == "attributes" &&
			r.URL.Query().Get("script-format") == "formatted" &&
			r.URL.Query().Get("offset") == "0" &&
			r.URL.Query().Get("limit") == "100" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
"tasks":[
	{
		"link": {"rel":"self", "href":"/kapacitor/v1/tasks/t1"},
		"id": "t1",
		"type":"stream",
		"dbrps":[{"db":"db","rp":"rp"}],
		"status" : "disabled",
		"executing" : false
	},
	{
		"link": {"rel":"self", "href":"/kapacitor/v1/tasks/t2"},
		"id": "t2",
		"type":"batch",
		"dbrps":[{"db":"db","rp":"rp"}],
		"status" : "enabled",
		"executing" : true,
		"stats": {
			"task-stats" : {
				"throughput" : 5.6
			},
			"node-stats" : {
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

	tasks, err := c.ListTasks(nil)
	if err != nil {
		t.Fatal(err)
	}
	exp := []client.Task{
		{
			Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/tasks/t1"},
			ID:   "t1",
			Type: client.StreamTask,
			DBRPs: []client.DBRP{{
				Database:        "db",
				RetentionPolicy: "rp",
			}},
			Status:         client.Disabled,
			Executing:      false,
			ExecutionStats: client.ExecutionStats{},
		},
		{
			Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/tasks/t2"},
			ID:   "t2",
			Type: client.BatchTask,
			DBRPs: []client.DBRP{{
				Database:        "db",
				RetentionPolicy: "rp",
			}},
			Status:    client.Enabled,
			Executing: true,
			ExecutionStats: client.ExecutionStats{
				TaskStats: map[string]interface{}{
					"throughput": 5.6,
				},
				NodeStats: map[string]map[string]interface{}{
					"stream1": map[string]interface{}{
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

func Test_ListTasks_Options(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/tasks" && r.Method == "GET" &&
			r.URL.Query().Get("pattern") == "t*" &&
			len(r.URL.Query()["fields"]) == 3 &&
			r.URL.Query()["fields"][0] == "status" &&
			r.URL.Query()["fields"][1] == "error" &&
			r.URL.Query()["fields"][2] == "executing" &&
			r.URL.Query().Get("dot-view") == "attributes" &&
			r.URL.Query().Get("script-format") == "formatted" &&
			r.URL.Query().Get("offset") == "100" &&
			r.URL.Query().Get("limit") == "100" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
"tasks":[
	{
		"link": {"rel":"self", "href":"/kapacitor/v1/tasks/t1"},
		"id": "t1",
		"status" : "enabled",
		"executing" : false,
		"error": "failed"
	},
	{
		"link": {"rel":"self", "href":"/kapacitor/v1/tasks/t2"},
		"id": "t2",
		"status" : "enabled",
		"executing" : true,
		"error": ""
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

	tasks, err := c.ListTasks(&client.ListTasksOptions{
		Pattern: "t*",
		Fields:  []string{"status", "error", "executing"},
		Offset:  100,
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := []client.Task{
		{
			Link:      client.Link{Relation: client.Self, Href: "/kapacitor/v1/tasks/t1"},
			ID:        "t1",
			Status:    client.Enabled,
			Executing: false,
			Error:     "failed",
		},
		{
			Link:      client.Link{Relation: client.Self, Href: "/kapacitor/v1/tasks/t2"},
			ID:        "t2",
			Status:    client.Enabled,
			Executing: true,
			Error:     "",
		},
	}
	if !reflect.DeepEqual(exp, tasks) {
		t.Errorf("unexpected task list: got:\n%v\nexp:\n%v", tasks, exp)
	}
}

func Test_TaskOutput(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/tasks/taskname/cpu" && r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
    "series": [
        {
            "name": "cpu",
            "columns": [
                "time",
                "value"
            ],
            "values": [
                [
                    "2015-01-29T21:55:43.702900257Z",
                    55
                ],
                [
                    "2015-01-29T21:56:43.702900257Z",
                    42
                ]
            ]
        }
    ]
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

	r, err := c.TaskOutput(c.TaskLink("taskname"), "cpu")
	if err != nil {
		t.Fatal(err)
	}
	exp := &influxql.Result{
		Series: models.Rows{{
			Name:    "cpu",
			Columns: []string{"time", "value"},
			Values: [][]interface{}{
				{
					"2015-01-29T21:55:43.702900257Z",
					55.0,
				},
				{
					"2015-01-29T21:56:43.702900257Z",
					42.0,
				},
			},
		}},
	}
	if !reflect.DeepEqual(exp, r) {
		t.Errorf("unexpected task output: \ngot\n%v\nexp\n%v\n", r, exp)
		t.Errorf("unexpected task output: \ngot.Series\n%v\nexp.Series\n%v\n", r.Series[0], exp.Series[0])
	}
}

func Test_Template(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/templates/t1" && r.Method == "GET" &&
			r.URL.Query().Get("script-format") == "formatted" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
	"link": {"rel":"self", "href":"/kapacitor/v1/templates/t1"},
	"type":"stream",
	"script":"var x = 5\nstream\n    |from()\n        .measurement('cpu')\n",
    "vars": {"x":{"value": 5, "type":"int"}},
	"dot": "digraph t1 {}",
	"error": ""
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

	template, err := c.Template(c.TemplateLink("t1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Template{
		Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/templates/t1"},
		Type: client.StreamTask,
		TICKscript: `var x = 5
stream
    |from()
        .measurement('cpu')
`,
		Dot:   "digraph t1 {}",
		Error: "",
		Vars: client.Vars{
			"x": {
				Type:  client.VarInt,
				Value: int64(5),
			},
		},
	}
	if !reflect.DeepEqual(exp, template) {
		t.Errorf("unexpected template:\ngot:\n%v\nexp:\n%v", template, exp)
	}
}

func Test_Template_RawFormat(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/templates/t1" && r.Method == "GET" &&
			r.URL.Query().Get("script-format") == "raw" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
	"link": {"rel":"self", "href":"/kapacitor/v1/templates/t1"},
	"type":"stream",
	"script":"stream|from().measurement('cpu')",
	"dot": "digraph t1 {\n}",
	"error": ""
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

	template, err := c.Template(c.TemplateLink("t1"), &client.TemplateOptions{ScriptFormat: "raw"})
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Template{
		Link:       client.Link{Relation: client.Self, Href: "/kapacitor/v1/templates/t1"},
		Type:       client.StreamTask,
		TICKscript: "stream|from().measurement('cpu')",
		Dot:        "digraph t1 {\n}",
		Error:      "",
	}
	if !reflect.DeepEqual(exp, template) {
		t.Errorf("unexpected template:\ngot:\n%v\nexp:\n%v", template, exp)
	}
}

func Test_CreateTemplate(t *testing.T) {
	tickScript := "stream|from().measurement('cpu')"
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var template client.CreateTemplateOptions
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body, &template)

		if r.URL.Path == "/kapacitor/v1/templates" && r.Method == "POST" {
			exp := client.CreateTemplateOptions{
				ID:         "templatename",
				Type:       client.StreamTask,
				TICKscript: tickScript,
			}
			if !reflect.DeepEqual(exp, template) {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "unexpected CreateTemplate body: got:\n%v\nexp:\n%v\n", template, exp)
			} else {
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, `{"link": {"rel":"self", "href":"/kapacitor/v1/templates/templatename"}, "id":"templatename"}`)
			}
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	template, err := c.CreateTemplate(client.CreateTemplateOptions{
		ID:         "templatename",
		Type:       client.StreamTask,
		TICKscript: tickScript,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, exp := template.Link.Href, "/kapacitor/v1/templates/templatename"; got != exp {
		t.Errorf("unexpected template link got %s exp %s", got, exp)
	}
	if got, exp := template.ID, "templatename"; got != exp {
		t.Errorf("unexpected template ID got %s exp %s", got, exp)
	}
}

func Test_UpdateTemplate(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var template client.UpdateTemplateOptions
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body, &template)

		if r.URL.Path == "/kapacitor/v1/templates/templatename" && r.Method == "PATCH" {
			exp := client.UpdateTemplateOptions{
				Type: client.BatchTask,
			}
			if !reflect.DeepEqual(exp, template) {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "unexpected UpdateTemplate body: got:\n%v\nexp:\n%v\n", template, exp)
			} else {
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, `{"link": {"rel":"self", "href":"/kapacitor/v1/templates/templatename"}, "id":"templatename"}`)
			}
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	template, err := c.UpdateTemplate(
		c.TemplateLink("templatename"),
		client.UpdateTemplateOptions{
			Type: client.BatchTask,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, exp := template.Link.Href, "/kapacitor/v1/templates/templatename"; got != exp {
		t.Errorf("unexpected template link got %s exp %s", got, exp)
	}
	if got, exp := template.ID, "templatename"; got != exp {
		t.Errorf("unexpected template ID got %s exp %s", got, exp)
	}
}

func Test_DeleteTemplate(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/templates/templatename" && r.Method == "DELETE" {
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

	err = c.DeleteTemplate(c.TemplateLink("templatename"))
	if err != nil {
		t.Fatal(err)
	}
}

func Test_ListTemplates(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/templates" && r.Method == "GET" &&
			r.URL.Query().Get("pattern") == "" &&
			r.URL.Query().Get("fields") == "" &&
			r.URL.Query().Get("script-format") == "formatted" &&
			r.URL.Query().Get("offset") == "0" &&
			r.URL.Query().Get("limit") == "100" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
"templates":[
	{
		"link": {"rel":"self", "href":"/kapacitor/v1/templates/t1"},
		"id": "t1",
		"type":"stream",
		"script": "stream|from()"
	},
	{
		"link": {"rel":"self", "href":"/kapacitor/v1/templates/t2"},
		"id": "t2",
		"type":"batch",
		"script": "batch|query()"
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

	templates, err := c.ListTemplates(nil)
	if err != nil {
		t.Fatal(err)
	}
	exp := []client.Template{
		{
			Link:       client.Link{Relation: client.Self, Href: "/kapacitor/v1/templates/t1"},
			ID:         "t1",
			Type:       client.StreamTask,
			TICKscript: "stream|from()",
		},
		{
			Link:       client.Link{Relation: client.Self, Href: "/kapacitor/v1/templates/t2"},
			ID:         "t2",
			Type:       client.BatchTask,
			TICKscript: "batch|query()",
		},
	}
	if !reflect.DeepEqual(exp, templates) {
		t.Errorf("unexpected template list: got:\n%v\nexp:\n%v", templates, exp)
	}
}

func Test_ListTemplates_Options(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/templates" && r.Method == "GET" &&
			r.URL.Query().Get("pattern") == "t*" &&
			len(r.URL.Query()["fields"]) == 1 &&
			r.URL.Query()["fields"][0] == "type" &&
			r.URL.Query().Get("script-format") == "formatted" &&
			r.URL.Query().Get("offset") == "100" &&
			r.URL.Query().Get("limit") == "100" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
"templates":[
	{
		"link": {"rel":"self", "href":"/kapacitor/v1/templates/t1"},
		"id": "t1",
		"type":"stream"
	},
	{
		"link": {"rel":"self", "href":"/kapacitor/v1/templates/t2"},
		"id": "t2",
		"type":"batch"
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

	templates, err := c.ListTemplates(&client.ListTemplatesOptions{
		Pattern: "t*",
		Fields:  []string{"type"},
		Offset:  100,
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := []client.Template{
		{
			Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/templates/t1"},
			ID:   "t1",
			Type: client.StreamTask,
		},
		{
			Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/templates/t2"},
			ID:   "t2",
			Type: client.BatchTask,
		},
	}
	if !reflect.DeepEqual(exp, templates) {
		t.Errorf("unexpected template list: got:\n%v\nexp:\n%v", templates, exp)
	}
}

func Test_RecordStream(t *testing.T) {
	stop := time.Now().Add(time.Minute).UTC()
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var opts client.RecordStreamOptions
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body, &opts)
		if r.URL.Path == "/kapacitor/v1/recordings/stream" && r.Method == "POST" &&
			opts.Task == "taskname" &&
			opts.Stop == stop {
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"link": {"rel":"self", "href":"/kapacitor/v1/recordings/rid1"}}`)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	r, err := c.RecordStream(client.RecordStreamOptions{
		Task: "taskname",
		Stop: stop,
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "/kapacitor/v1/recordings/rid1", string(r.Link.Href); got != exp {
		t.Errorf("unexpected recording id for test: got: %s exp: %s", got, exp)
	}
}
func Test_RecordBatch(t *testing.T) {
	stop := time.Now().UTC()
	start := stop.Add(-24 * time.Hour)
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var opts client.RecordBatchOptions
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body, &opts)
		if r.URL.Path == "/kapacitor/v1/recordings/batch" && r.Method == "POST" &&
			opts.Task == "taskname" &&
			opts.Start == start &&
			opts.Stop == stop {
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"link": {"rel":"self", "href":"/kapacitor/v1/recordings/rid1"}, "id":"rid1"}`)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	r, err := c.RecordBatch(client.RecordBatchOptions{
		Task:  "taskname",
		Start: start,
		Stop:  stop,
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "/kapacitor/v1/recordings/rid1", string(r.Link.Href); got != exp {
		t.Errorf("unexpected recording link for test: got: %s exp: %s", got, exp)
	}
	if exp, got := "rid1", r.ID; got != exp {
		t.Errorf("unexpected recording ID for test: got: %s exp: %s", got, exp)
	}
}

func Test_RecordQuery(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var opts client.RecordQueryOptions
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body, &opts)
		if r.URL.Path == "/kapacitor/v1/recordings/query" && r.Method == "POST" &&
			opts.Query == "SELECT * FROM allthethings" &&
			opts.Type == client.StreamTask &&
			opts.Cluster == "mycluster" {
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"link": {"rel":"self", "href":"/kapacitor/v1/recordings/rid1"},"id":"rid1"}`)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v body: %s", r, string(body))
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	r, err := c.RecordQuery(client.RecordQueryOptions{
		Query:   "SELECT * FROM allthethings",
		Cluster: "mycluster",
		Type:    client.StreamTask,
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "/kapacitor/v1/recordings/rid1", string(r.Link.Href); got != exp {
		t.Errorf("unexpected recording id for test: got: %s exp: %s", got, exp)
	}
	if exp, got := "rid1", r.ID; got != exp {
		t.Errorf("unexpected recording ID for test: got: %s exp: %s", got, exp)
	}
}

func Test_Recording(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/recordings/rid1" && r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
	"link": {"rel":"self", "href":"/kapacitor/v1/recordings/rid1"},
	"id": "rid1",
	"type":"batch",
	"size": 42,
	"date" : "2016-03-31T11:24:55.526388889Z",
	"error": "",
	"status": "finished",
	"progress": 1.0
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

	recordings, err := c.Recording(c.RecordingLink("rid1"))
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Recording{
		Link:     client.Link{Relation: client.Self, Href: "/kapacitor/v1/recordings/rid1"},
		ID:       "rid1",
		Type:     client.BatchTask,
		Size:     42,
		Date:     time.Date(2016, 3, 31, 11, 24, 55, 526388889, time.UTC),
		Status:   client.Finished,
		Progress: 1.0,
	}
	if !reflect.DeepEqual(exp, recordings) {
		t.Errorf("unexpected recording list:\ngot:\n%v\nexp:\n%v", recordings, exp)
	}
}

func Test_RecordingRunning(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/recordings/rid1" && r.Method == "GET" {
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprintf(w, `{
	"link": {"rel":"self", "href":"/kapacitor/v1/recordings/rid1"},
	"id": "rid1",
	"type":"batch",
	"size": 42,
	"date" : "2016-03-31T11:24:55.526388889Z",
	"error": "",
	"status": "running",
	"progress": 0.42
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

	recordings, err := c.Recording(c.RecordingLink("rid1"))
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Recording{
		Link:     client.Link{Relation: client.Self, Href: "/kapacitor/v1/recordings/rid1"},
		ID:       "rid1",
		Type:     client.BatchTask,
		Size:     42,
		Date:     time.Date(2016, 3, 31, 11, 24, 55, 526388889, time.UTC),
		Status:   client.Running,
		Progress: 0.42,
	}
	if !reflect.DeepEqual(exp, recordings) {
		t.Errorf("unexpected recording list:\ngot:\n%v\nexp:\n%v", recordings, exp)
	}
}

func Test_ListRecordings(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/recordings" && r.Method == "GET" &&
			r.URL.Query().Get("pattern") == "" &&
			r.URL.Query().Get("offset") == "0" &&
			r.URL.Query().Get("limit") == "100" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
"recordings":[
	{
		"link": {"rel":"self", "href":"/kapacitor/v1/recordings/rid1"},
		"id": "rid1",
		"type":"batch",
		"size": 42,
		"date" : "2016-03-31T11:24:55.526388889Z",
		"error": "",
		"status": "running",
		"progress": 0.67
	},
	{
		"link": {"rel":"self", "href":"/kapacitor/v1/recordings/rid2"},
		"id": "rid2",
		"type":"stream",
		"size": 4200,
		"date" : "2016-03-31T10:24:55.526388889Z",
		"error": "",
		"status": "finished",
		"progress": 1.0
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

	tasks, err := c.ListRecordings(nil)
	if err != nil {
		t.Fatal(err)
	}
	exp := []client.Recording{
		{
			Link:     client.Link{Relation: client.Self, Href: "/kapacitor/v1/recordings/rid1"},
			ID:       "rid1",
			Type:     client.BatchTask,
			Size:     42,
			Date:     time.Date(2016, 3, 31, 11, 24, 55, 526388889, time.UTC),
			Status:   client.Running,
			Progress: 0.67,
		},
		{
			Link:     client.Link{Relation: client.Self, Href: "/kapacitor/v1/recordings/rid2"},
			ID:       "rid2",
			Type:     client.StreamTask,
			Size:     4200,
			Date:     time.Date(2016, 3, 31, 10, 24, 55, 526388889, time.UTC),
			Status:   client.Finished,
			Progress: 1.0,
		},
	}
	if !reflect.DeepEqual(exp, tasks) {
		t.Errorf("unexpected recording list:\ngot:\n%v\nexp:\n%v", tasks, exp)
	}
}

func Test_ListRecordings_Filter(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/recordings" && r.Method == "GET" &&
			r.URL.Query().Get("pattern") == "rid1" &&
			len(r.URL.Query()["fields"]) == 3 &&
			r.URL.Query()["fields"][0] == "status" &&
			r.URL.Query()["fields"][1] == "error" &&
			r.URL.Query()["fields"][2] == "progress" &&
			r.URL.Query().Get("offset") == "0" &&
			r.URL.Query().Get("limit") == "1" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
"recordings":[
	{
		"link": {"rel":"self", "href":"/kapacitor/v1/recordings/rid1"},
		"id": "rid1",
		"error": "",
		"status": "running",
		"progress": 0.67
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

	tasks, err := c.ListRecordings(&client.ListRecordingsOptions{
		Pattern: "rid1",
		Fields:  []string{"status", "error", "progress"},
		Limit:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := []client.Recording{
		{
			Link:     client.Link{Relation: client.Self, Href: "/kapacitor/v1/recordings/rid1"},
			ID:       "rid1",
			Status:   client.Running,
			Progress: 0.67,
		},
	}
	if !reflect.DeepEqual(exp, tasks) {
		t.Errorf("unexpected recording list:\ngot:\n%v\nexp:\n%v", tasks, exp)
	}
}

func Test_DeleteRecording(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/recordings/rid1" && r.Method == "DELETE" {
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

	err = c.DeleteRecording(c.RecordingLink("rid1"))
	if err != nil {
		t.Fatal(err)
	}
}
func Test_Replay(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/replays/replayid" && r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
		"link": {"rel":"self", "href":"/kapacitor/v1/replays/replayid"},
		"id": "replayid",
		"task": "taskid",
		"recording": "recordingid",
		"recording-time":false,
		"clock": "fast",
		"error": "",
		"status": "finished",
		"progress": 1.0
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

	replay, err := c.Replay(c.ReplayLink("replayid"))
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Replay{
		Link:          client.Link{Relation: client.Self, Href: "/kapacitor/v1/replays/replayid"},
		ID:            "replayid",
		Task:          "taskid",
		Recording:     "recordingid",
		RecordingTime: false,
		Clock:         client.Fast,
		Error:         "",
		Status:        client.Finished,
		Progress:      1.0,
	}
	if !reflect.DeepEqual(exp, replay) {
		t.Errorf("unexpected replay got: %v exp %v", replay, exp)
	}
}

func Test_ReplayRunning(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/replays/replayid" && r.Method == "GET" {
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprintf(w, `{
		"link": {"rel":"self", "href":"/kapacitor/v1/replays/replayid"},
		"id": "replayid",
		"task": "taskid",
		"recording": "recordingid",
		"recording-time":false,
		"clock": "fast",
		"error": "",
		"status": "running",
		"progress": 0.67
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

	replay, err := c.Replay(c.ReplayLink("replayid"))
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Replay{
		Link:          client.Link{Relation: client.Self, Href: "/kapacitor/v1/replays/replayid"},
		ID:            "replayid",
		Task:          "taskid",
		Recording:     "recordingid",
		RecordingTime: false,
		Clock:         client.Fast,
		Error:         "",
		Status:        client.Running,
		Progress:      0.67,
	}
	if !reflect.DeepEqual(exp, replay) {
		t.Errorf("unexpected replay got: %v exp %v", replay, exp)
	}
}

func Test_CreateReplay(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var opts client.CreateReplayOptions
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body, &opts)
		if r.URL.Path == "/kapacitor/v1/replays" && r.Method == "POST" &&
			opts.Task == "taskname" &&
			opts.Recording == "recording" &&
			opts.RecordingTime == false &&
			opts.Clock == client.Fast {
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"link":{"rel":"self","href":"/kapacitor/v1/replays/replayid"}, "id":"replayid"}`)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	replay, err := c.CreateReplay(client.CreateReplayOptions{
		Task:      "taskname",
		Recording: "recording",
		Clock:     client.Fast,
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "/kapacitor/v1/replays/replayid", string(replay.Link.Href); exp != got {
		t.Errorf("unexpected replay.Link.Href got %s exp %s", got, exp)
	}
	if exp, got := "replayid", replay.ID; exp != got {
		t.Errorf("unexpected replay.ID got %s exp %s", got, exp)
	}
}

func Test_ReplayBatch(t *testing.T) {
	stop := time.Now().UTC()
	start := stop.Add(-24 * time.Hour)
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var opts client.ReplayBatchOptions
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body, &opts)
		if r.URL.Path == "/kapacitor/v1/replays/batch" && r.Method == "POST" &&
			opts.Task == "taskname" &&
			opts.Start == start &&
			opts.Stop == stop &&
			opts.RecordingTime == true &&
			opts.Clock == client.Real {
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"link":{"rel":"self","href":"/kapacitor/v1/replays/replayid"}, "id":"replayid"}`)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	replay, err := c.ReplayBatch(client.ReplayBatchOptions{
		Task:          "taskname",
		Start:         start,
		Stop:          stop,
		Clock:         client.Real,
		RecordingTime: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "/kapacitor/v1/replays/replayid", string(replay.Link.Href); exp != got {
		t.Errorf("unexpected replay.Link.Href got %s exp %s", got, exp)
	}
	if exp, got := "replayid", replay.ID; exp != got {
		t.Errorf("unexpected replay.ID got %s exp %s", got, exp)
	}
}

func Test_ReplayQuery(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var opts client.ReplayQueryOptions
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body, &opts)
		if r.URL.Path == "/kapacitor/v1/replays/query" && r.Method == "POST" &&
			opts.Task == "taskname" &&
			opts.Query == "SELECT * FROM allthethings" &&
			opts.Cluster == "mycluster" &&
			opts.RecordingTime == false &&
			opts.Clock == client.Fast {
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"link":{"rel":"self","href":"/kapacitor/v1/replays/replayid"}, "id":"replayid"}`)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	replay, err := c.ReplayQuery(client.ReplayQueryOptions{
		Task:    "taskname",
		Query:   "SELECT * FROM allthethings",
		Cluster: "mycluster",
		Clock:   client.Fast,
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "/kapacitor/v1/replays/replayid", string(replay.Link.Href); exp != got {
		t.Errorf("unexpected replay.Link.Href got %s exp %s", got, exp)
	}
	if exp, got := "replayid", replay.ID; exp != got {
		t.Errorf("unexpected replay.ID got %s exp %s", got, exp)
	}
}

func Test_DeleteReplay(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/replays/replayid" && r.Method == "DELETE" {
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

	err = c.DeleteReplay(c.ReplayLink("replayid"))
	if err != nil {
		t.Fatal(err)
	}
}

func Test_ListReplays(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/replays" && r.Method == "GET" &&
			r.URL.Query().Get("pattern") == "" &&
			r.URL.Query().Get("offset") == "0" &&
			r.URL.Query().Get("limit") == "100" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
"replays":[
	{
		"link": {"rel":"self", "href":"/kapacitor/v1/replays/rpid1"},
		"id": "rpid1",
		"task": "taskid",
		"recording" : "recordingid",
		"clock": "fast",
		"recording-time": true,
		"error": "",
		"status": "running",
		"progress": 0.67
	},
	{
		"link": {"rel":"self", "href":"/kapacitor/v1/replays/rpid2"},
		"id": "rpid2",
		"task": "taskid2",
		"recording" : "recordingid2",
		"clock": "real",
		"recording-time": false,
		"error": "",
		"status": "finished",
		"progress": 1.0
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

	tasks, err := c.ListReplays(nil)
	if err != nil {
		t.Fatal(err)
	}
	exp := []client.Replay{
		{
			Link:          client.Link{Relation: client.Self, Href: "/kapacitor/v1/replays/rpid1"},
			ID:            "rpid1",
			Task:          "taskid",
			Recording:     "recordingid",
			Clock:         client.Fast,
			RecordingTime: true,
			Status:        client.Running,
			Progress:      0.67,
		},
		{
			Link:          client.Link{Relation: client.Self, Href: "/kapacitor/v1/replays/rpid2"},
			ID:            "rpid2",
			Task:          "taskid2",
			Recording:     "recordingid2",
			Clock:         client.Real,
			RecordingTime: false,
			Status:        client.Finished,
			Progress:      1.0,
		},
	}
	if !reflect.DeepEqual(exp, tasks) {
		t.Errorf("unexpected replay list:\ngot:\n%v\nexp:\n%v", tasks, exp)
	}
}

func Test_ListReplays_Filter(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/replays" && r.Method == "GET" &&
			r.URL.Query().Get("pattern") == "rpid1" &&
			len(r.URL.Query()["fields"]) == 3 &&
			r.URL.Query()["fields"][0] == "status" &&
			r.URL.Query()["fields"][1] == "error" &&
			r.URL.Query()["fields"][2] == "progress" &&
			r.URL.Query().Get("offset") == "0" &&
			r.URL.Query().Get("limit") == "1" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
"replays":[
	{
		"link": {"rel":"self", "href":"/kapacitor/v1/replays/rpid1"},
		"id": "rpid1",
		"error": "",
		"status": "running",
		"progress": 0.67
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

	tasks, err := c.ListReplays(&client.ListReplaysOptions{
		Pattern: "rpid1",
		Fields:  []string{"status", "error", "progress"},
		Limit:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := []client.Replay{
		{
			Link:     client.Link{Relation: client.Self, Href: "/kapacitor/v1/replays/rpid1"},
			ID:       "rpid1",
			Status:   client.Running,
			Progress: 0.67,
		},
	}
	if !reflect.DeepEqual(exp, tasks) {
		t.Errorf("unexpected replay list:\ngot:\n%v\nexp:\n%v", tasks, exp)
	}
}

func Test_LogLevel(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var opts client.LogLevelOptions
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body, &opts)

		if r.URL.Path == "/kapacitor/v1/loglevel" && r.Method == "POST" &&
			opts.Level == "DEBUG" {
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

func Test_Bad_Creds(t *testing.T) {
	testCases := []struct {
		creds *client.Credentials
		err   error
	}{
		{
			creds: &client.Credentials{
				Method: client.UserAuthentication,
			},
			err: errors.New("invalid credentials: missing username"),
		},
		{
			creds: &client.Credentials{
				Method:   client.UserAuthentication,
				Username: "bob",
			},
			err: errors.New("invalid credentials: missing password"),
		},
		{
			creds: &client.Credentials{
				Method: client.BearerAuthentication,
			},
			err: errors.New("invalid credentials: missing token"),
		},
	}
	for _, tc := range testCases {
		if _, err := client.New(
			client.Config{
				URL:         "http://localhost",
				Credentials: tc.creds,
			},
		); err == nil {
			t.Error("expected credential error")
		} else if exp, got := tc.err.Error(), err.Error(); got != exp {
			t.Errorf("unexpected error message: got %q exp %q", got, exp)
		}
	}
}

func Test_UserAuthentication(t *testing.T) {
	s, c, err := newClientWithConfig(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, auth := r.BasicAuth()
		if r.URL.Path == "/kapacitor/v1/ping" && r.Method == "GET" &&
			auth &&
			u == "bob" &&
			p == "don't look" {
			w.Header().Set("X-Kapacitor-Version", "versionStr")
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}), client.Config{
		Credentials: &client.Credentials{
			Method:   client.UserAuthentication,
			Username: "bob",
			Password: "don't look",
		},
	})
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

func Test_BearerAuthentication(t *testing.T) {
	s, c, err := newClientWithConfig(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if r.URL.Path == "/kapacitor/v1/ping" && r.Method == "GET" &&
			auth == "Bearer myfake.token" {
			w.Header().Set("X-Kapacitor-Version", "versionStr")
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "request: %v", r)
		}
	}), client.Config{
		Credentials: &client.Credentials{
			Method: client.BearerAuthentication,
			Token:  "myfake.token",
		},
	})
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
