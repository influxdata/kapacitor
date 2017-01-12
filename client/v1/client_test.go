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
			name: "ConfigSections",
			fnc: func(c *client.Client) error {
				_, err := c.ConfigSections()
				return err
			},
		},
		{
			name: "ConfigSection",
			fnc: func(c *client.Client) error {
				_, err := c.ConfigSection(c.ConfigSectionLink(""))
				return err
			},
		},
		{
			name: "ConfigElement",
			fnc: func(c *client.Client) error {
				_, err := c.ConfigElement(c.ConfigElementLink("", ""))
				return err
			},
		},
		{
			name: "ConfigUpdate",
			fnc: func(c *client.Client) error {
				err := c.ConfigUpdate(c.ConfigSectionLink(""), client.ConfigUpdateAction{})
				return err
			},
		},
		{
			name: "ServiceTests",
			fnc: func(c *client.Client) error {
				_, err := c.ListServiceTests(nil)
				return err
			},
		},
		{
			name: "ServiceTest",
			fnc: func(c *client.Client) error {
				_, err := c.ServiceTest(c.ServiceTestLink(""))
				return err
			},
		},
		{
			name: "DoServiceTest",
			fnc: func(c *client.Client) error {
				_, err := c.DoServiceTest(c.ServiceTestLink(""), nil)
				return err
			},
		},
		{
			name: "Topic",
			fnc: func(c *client.Client) error {
				_, err := c.Topic(c.TopicLink(""))
				return err
			},
		},
		{
			name: "ListTopics",
			fnc: func(c *client.Client) error {
				_, err := c.ListTopics(nil)
				return err
			},
		},
		{
			name: "DeleteTopic",
			fnc: func(c *client.Client) error {
				err := c.DeleteTopic(c.TopicLink(""))
				return err
			},
		},
		{
			name: "TopicEvent",
			fnc: func(c *client.Client) error {
				_, err := c.TopicEvent(c.TopicEventLink("topic", "event"))
				return err
			},
		},
		{
			name: "ListTopicEvents",
			fnc: func(c *client.Client) error {
				_, err := c.ListTopicEvents(c.TopicEventsLink(""), nil)
				return err
			},
		},
		{
			name: "ListTopicHandlers",
			fnc: func(c *client.Client) error {
				_, err := c.ListTopicHandlers(c.TopicHandlersLink(""))
				return err
			},
		},
		{
			name: "Handler",
			fnc: func(c *client.Client) error {
				_, err := c.Handler(c.HandlerLink(""))
				return err
			},
		},
		{
			name: "CreateHandler",
			fnc: func(c *client.Client) error {
				_, err := c.CreateHandler(client.HandlerOptions{})
				return err
			},
		},
		{
			name: "PatchHandler",
			fnc: func(c *client.Client) error {
				_, err := c.PatchHandler(c.HandlerLink(""), nil)
				return err
			},
		},
		{
			name: "ReplaceHandler",
			fnc: func(c *client.Client) error {
				_, err := c.ReplaceHandler(c.HandlerLink(""), client.HandlerOptions{})
				return err
			},
		},
		{
			name: "DeleteHandler",
			fnc: func(c *client.Client) error {
				err := c.DeleteHandler(c.HandlerLink(""))
				return err
			},
		},
		{
			name: "ListHandlers",
			fnc: func(c *client.Client) error {
				_, err := c.ListHandlers(nil)
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

func Test_ConfigUpdate(t *testing.T) {
	expUpdate := client.ConfigUpdateAction{
		Set: map[string]interface{}{
			"option": "new value",
		},
	}
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var update client.ConfigUpdateAction
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body, &update)
		if r.URL.Path == "/kapacitor/v1/config/section" && r.Method == "POST" &&
			reflect.DeepEqual(update, expUpdate) {
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

	if err := c.ConfigUpdate(c.ConfigSectionLink("section"), expUpdate); err != nil {
		t.Fatal(err)
	}
}

func Test_ConfigUpdate_Element(t *testing.T) {
	expUpdate := client.ConfigUpdateAction{
		Set: map[string]interface{}{
			"option": "new value",
		},
	}
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var update client.ConfigUpdateAction
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body, &update)
		if r.URL.Path == "/kapacitor/v1/config/section/element" && r.Method == "POST" &&
			reflect.DeepEqual(update, expUpdate) {
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

	if err := c.ConfigUpdate(c.ConfigElementLink("section", "element"), expUpdate); err != nil {
		t.Fatal(err)
	}
}

func Test_ConfigSections(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/config" && r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
	"link": {"rel":"self", "href":"/kapacitor/v1/config"},
	"sections":{
		"sectionA": {
			"link": {"rel":"self", "href":"/kapacitor/v1/config/sectionA"},
			"elements": [
				{
					"link": {"rel":"self", "href":"/kapacitor/v1/config/sectionA/A"},
					"options" :{
						"name": "A",
						"optionA": "o1",
						"optionB": "o2",
						"optionC": "o3",
						"optionD": "o4"
					}
				},
				{
					"link": {"rel":"self", "href":"/kapacitor/v1/config/sectionA/B"},
					"options" :{
						"name": "B",
						"optionA": "o5",
						"optionB": "o6",
						"optionC": "o7",
						"optionD": "o8"
					}
				}
			]
		},
		"sectionB": {
			"link": {"rel":"self", "href":"/kapacitor/v1/config/sectionB"},
			"elements" :[
				{
					"link": {"rel":"self", "href":"/kapacitor/v1/config/sectionB/X"},
					"options" :{
						"name": "X",
						"optionA": "o1",
						"optionB": "o2",
						"optionC": "o3",
						"optionD": "o4"
					}
				},
				{
					"link": {"rel":"self", "href":"/kapacitor/v1/config/sectionB/Y"},
					"options" :{
						"name": "Y",
						"optionH": "o5",
						"optionJ": "o6",
						"optionK": "o7",
						"optionL": "o8"
					}
				}
			]
		},
		"sectionC": {
			"link": {"rel":"self", "href":"/kapacitor/v1/config/sectionC"},
			"elements" :[{
				"link": {"rel":"self", "href":"/kapacitor/v1/config/sectionC/"},
				"options" :{
					"optionA": "o1",
					"optionB": "o2",
					"optionC": "o3",
					"optionD": "o4"
				},
				"redacted" :["optionC"]
			}]
		}
	}
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

	sections, err := c.ConfigSections()
	if err != nil {
		t.Fatal(err)
	}
	exp := client.ConfigSections{
		Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config"},
		Sections: map[string]client.ConfigSection{
			"sectionA": client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sectionA"},
				Elements: []client.ConfigElement{
					{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sectionA/A"},
						Options: map[string]interface{}{
							"name":    "A",
							"optionA": "o1",
							"optionB": "o2",
							"optionC": "o3",
							"optionD": "o4",
						},
					},
					{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sectionA/B"},
						Options: map[string]interface{}{
							"name":    "B",
							"optionA": "o5",
							"optionB": "o6",
							"optionC": "o7",
							"optionD": "o8",
						},
					},
				},
			},
			"sectionB": client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sectionB"},
				Elements: []client.ConfigElement{
					{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sectionB/X"},
						Options: map[string]interface{}{
							"name":    "X",
							"optionA": "o1",
							"optionB": "o2",
							"optionC": "o3",
							"optionD": "o4",
						},
					},
					{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sectionB/Y"},
						Options: map[string]interface{}{
							"name":    "Y",
							"optionH": "o5",
							"optionJ": "o6",
							"optionK": "o7",
							"optionL": "o8",
						},
					},
				},
			},
			"sectionC": client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sectionC"},
				Elements: []client.ConfigElement{
					{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sectionC/"},
						Options: map[string]interface{}{
							"optionA": "o1",
							"optionB": "o2",
							"optionC": "o3",
							"optionD": "o4",
						},
						Redacted: []string{
							"optionC",
						},
					},
				},
			},
		},
	}
	if !reflect.DeepEqual(exp, sections) {
		t.Errorf("unexpected config section:\ngot:\n%v\nexp:\n%v", sections, exp)
	}
}

func Test_ConfigSection(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/config/section" && r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
	"link": {"rel":"self", "href":"/kapacitor/v1/config/section"},
	"elements" : [
		{
			"link": {"rel":"self", "href":"/kapacitor/v1/config/section/A"},
			"options": {
				"name": "A",
				"optionA": "o1",
				"optionB": "o2",
				"optionC": "o3",
				"optionD": "o4"
			}
		},
		{
			"link": {"rel":"self", "href":"/kapacitor/v1/config/section/B"},
			"options": {
				"name": "B",
				"optionA": "o5",
				"optionB": "o6",
				"optionC": "o7",
				"optionD": "o8"
			}
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

	section, err := c.ConfigSection(c.ConfigSectionLink("section"))
	if err != nil {
		t.Fatal(err)
	}
	exp := client.ConfigSection{
		Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/section"},
		Elements: []client.ConfigElement{
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/section/A"},
				Options: map[string]interface{}{
					"name":    "A",
					"optionA": "o1",
					"optionB": "o2",
					"optionC": "o3",
					"optionD": "o4",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/section/B"},
				Options: map[string]interface{}{
					"name":    "B",
					"optionA": "o5",
					"optionB": "o6",
					"optionC": "o7",
					"optionD": "o8",
				},
			},
		},
	}
	if !reflect.DeepEqual(exp, section) {
		t.Errorf("unexpected config section:\ngot:\n%v\nexp:\n%v", section, exp)
	}
}
func Test_ServiceTests(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/service-tests" && r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
	"link": {"rel":"self", "href": "/kapacitor/v1/service-tests"},
	"services" : [
		{
			"link": {"rel":"self", "href": "/kapacitor/v1/service-tests/influxdb"},
			"name": "influxdb",
			"options": {
				"cluster": ""
			}
		},
		{
			"link": {"rel":"self", "href": "/kapacitor/v1/service-tests/slack"},
			"name": "slack",
			"options": {
				"message": "test slack message",
				"channel": "#alerts",
				"level": "CRITICAL"
			}
		},
		{
			"link": {"rel":"self", "href": "/kapacitor/v1/service-tests/smtp"},
			"name": "smtp",
			"options": {
				"to": ["user@example.com"],
				"subject": "test subject",
				"body": "test body"
			}
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

	serviceTests, err := c.ListServiceTests(nil)
	if err != nil {
		t.Fatal(err)
	}
	exp := client.ServiceTests{
		Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests"},
		Services: []client.ServiceTest{
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/influxdb"},
				Name: "influxdb",
				Options: map[string]interface{}{
					"cluster": "",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/slack"},
				Name: "slack",
				Options: map[string]interface{}{
					"message": "test slack message",
					"channel": "#alerts",
					"level":   "CRITICAL",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/smtp"},
				Name: "smtp",
				Options: map[string]interface{}{
					"to":      []interface{}{"user@example.com"},
					"subject": "test subject",
					"body":    "test body",
				},
			},
		},
	}
	if !reflect.DeepEqual(exp, serviceTests) {
		t.Errorf("unexpected service tests:\ngot:\n%v\nexp:\n%v", serviceTests, exp)
	}
}
func Test_ServiceTest(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kapacitor/v1/service-tests/slack" && r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
	"link": {"rel":"self", "href": "/kapacitor/v1/service-tests/slack"},
	"name": "slack",
	"options": {
		"message": "test slack message",
		"channel": "#alerts",
		"level": "CRITICAL"
	}
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

	serviceTest, err := c.ServiceTest(c.ServiceTestLink("slack"))
	if err != nil {
		t.Fatal(err)
	}
	exp := client.ServiceTest{
		Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/slack"},
		Name: "slack",
		Options: map[string]interface{}{
			"message": "test slack message",
			"channel": "#alerts",
			"level":   "CRITICAL",
		},
	}
	if !reflect.DeepEqual(exp, serviceTest) {
		t.Errorf("unexpected service test:\ngot:\n%v\nexp:\n%v", serviceTest, exp)
	}
}
func Test_DoServiceTest(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		options := make(client.ServiceTestOptions)
		json.NewDecoder(r.Body).Decode(&options)
		expOptions := client.ServiceTestOptions{
			"message": "this is a slack test message",
			"channel": "@test_user",
		}

		if r.URL.Path == "/kapacitor/v1/service-tests/slack" &&
			r.Method == "POST" &&
			reflect.DeepEqual(expOptions, options) {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
	"success": true,
	"message": ""
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

	tr, err := c.DoServiceTest(c.ServiceTestLink("slack"), client.ServiceTestOptions{
		"message": "this is a slack test message",
		"channel": "@test_user",
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := client.ServiceTestResult{
		Success: true,
		Message: "",
	}
	if !reflect.DeepEqual(exp, tr) {
		t.Errorf("unexpected service test result:\ngot:\n%v\nexp:\n%v", tr, exp)
	}
}

func Test_Topic(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() == "/kapacitor/v1preview/alerts/topics/system" &&
			r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
    "link": {"rel":"self","href":"/kapacitor/v1preview/alerts/topics/system"},
    "events-link" : {"rel":"events","href":"/kapacitor/v1preview/alerts/topics/system/events"},
    "handlers-link": {"rel":"handlers","href":"/kapacitor/v1preview/alerts/topics/system/handlers"},
    "id": "system",
    "level":"CRITICAL",
	"collected": 5
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

	topic, err := c.Topic(c.TopicLink("system"))
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Topic{
		ID:           "system",
		Link:         client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/topics/system"},
		EventsLink:   client.Link{Relation: "events", Href: "/kapacitor/v1preview/alerts/topics/system/events"},
		HandlersLink: client.Link{Relation: "handlers", Href: "/kapacitor/v1preview/alerts/topics/system/handlers"},
		Level:        "CRITICAL",
		Collected:    5,
	}
	if !reflect.DeepEqual(exp, topic) {
		t.Errorf("unexpected topic result:\ngot:\n%v\nexp:\n%v", topic, exp)
	}
}

func Test_ListTopics(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() == "/kapacitor/v1preview/alerts/topics?min-level=WARNING&pattern=%2A" &&
			r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
    "link": {"rel":"self","href":"/kapacitor/v1preview/alerts/topics"},
    "topics": [
        {
            "link": {"rel":"self","href":"/kapacitor/v1preview/alerts/topics/system"},
            "events-link" : {"rel":"events","href":"/kapacitor/v1preview/alerts/topics/system/events"},
            "handlers-link": {"rel":"handlers","href":"/kapacitor/v1preview/alerts/topics/system/handlers"},
            "id": "system",
            "level":"CRITICAL"
        },
        {
            "link": {"rel":"self","href":"/kapacitor/v1preview/alerts/topics/app"},
            "events-link" : {"rel":"events","href":"/kapacitor/v1preview/alerts/topics/app/events"},
            "handlers-link": {"rel":"handlers","href":"/kapacitor/v1preview/alerts/topics/app/handlers"},
            "id": "app",
            "level":"WARNING"
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

	topics, err := c.ListTopics(&client.ListTopicsOptions{
		Pattern:  "*",
		MinLevel: "WARNING",
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Topics{
		Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/topics"},
		Topics: []client.Topic{
			{
				ID:           "system",
				Link:         client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/topics/system"},
				EventsLink:   client.Link{Relation: "events", Href: "/kapacitor/v1preview/alerts/topics/system/events"},
				HandlersLink: client.Link{Relation: "handlers", Href: "/kapacitor/v1preview/alerts/topics/system/handlers"},
				Level:        "CRITICAL",
			},
			{
				ID:           "app",
				Link:         client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/topics/app"},
				EventsLink:   client.Link{Relation: "events", Href: "/kapacitor/v1preview/alerts/topics/app/events"},
				HandlersLink: client.Link{Relation: "handlers", Href: "/kapacitor/v1preview/alerts/topics/app/handlers"},
				Level:        "WARNING",
			},
		},
	}
	if !reflect.DeepEqual(exp, topics) {
		t.Errorf("unexpected  topics result:\ngot:\n%v\nexp:\n%v", topics, exp)
	}
}

func Test_DeleteTopic(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() == "/kapacitor/v1preview/alerts/topics/system" &&
			r.Method == "DELETE" {
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

	err = c.DeleteTopic(c.TopicLink("system"))
	if err != nil {
		t.Fatal(err)
	}
}

func Test_TopicEvent(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() == "/kapacitor/v1preview/alerts/topics/system/events/cpu" &&
			r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
    "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/topics/system/events/cpu"},
    "id": "cpu",
    "state": {
        "level": "WARNING",
        "message": "cpu is WARNING",
        "time": "2016-12-01T00:00:00Z",
        "duration": "5m"
    }
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

	topicEvent, err := c.TopicEvent(c.TopicEventLink("system", "cpu"))
	if err != nil {
		t.Fatal(err)
	}
	exp := client.TopicEvent{
		ID:   "cpu",
		Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/topics/system/events/cpu"},
		State: client.EventState{
			Message:  "cpu is WARNING",
			Time:     time.Date(2016, 12, 1, 0, 0, 0, 0, time.UTC),
			Duration: client.Duration(5 * time.Minute),
			Level:    "WARNING",
		},
	}
	if !reflect.DeepEqual(exp, topicEvent) {
		t.Errorf("unexpected  topic event result:\ngot:\n%v\nexp:\n%v", topicEvent, exp)
	}
}

func Test_ListTopicEvents(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() == "/kapacitor/v1preview/alerts/topics/system/events?min-level=OK" &&
			r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
    "link": {"rel":"self","href":"/kapacitor/v1preview/alerts/topics/system/events?min-level=OK"},
    "topic": "system",
    "events": [
        {
            "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/topics/system/events/cpu"},
            "id": "cpu",
            "state": {
                "level": "WARNING",
                "message": "cpu is WARNING",
                "time": "2016-12-01T00:00:00Z",
                "duration": "5m"
            }
        },
        {
            "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/topics/system/events/mem"},
            "id": "mem",
            "state": {
                "level": "CRITICAL",
                "message": "mem is CRITICAL",
                "time": "2016-12-01T00:10:00Z",
                "duration": "1m"
            }
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

	topicEvents, err := c.ListTopicEvents(c.TopicEventsLink("system"), nil)
	if err != nil {
		t.Fatal(err)
	}
	exp := client.TopicEvents{
		Link:  client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/topics/system/events?min-level=OK"},
		Topic: "system",
		Events: []client.TopicEvent{
			{
				ID:   "cpu",
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/topics/system/events/cpu"},
				State: client.EventState{
					Message:  "cpu is WARNING",
					Time:     time.Date(2016, 12, 1, 0, 0, 0, 0, time.UTC),
					Duration: client.Duration(5 * time.Minute),
					Level:    "WARNING",
				},
			},
			{
				ID:   "mem",
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/topics/system/events/mem"},
				State: client.EventState{
					Message:  "mem is CRITICAL",
					Time:     time.Date(2016, 12, 1, 0, 10, 0, 0, time.UTC),
					Duration: client.Duration(1 * time.Minute),
					Level:    "CRITICAL",
				},
			},
		},
	}
	if !reflect.DeepEqual(exp, topicEvents) {
		t.Errorf("unexpected  topic events result:\ngot:\n%v\nexp:\n%v", topicEvents, exp)
	}
}
func Test_ListTopicHandlers(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() == "/kapacitor/v1preview/alerts/topics/system/handlers" &&
			r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
    "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/topics/system/handlers"},
    "topic": "system",
    "handlers": [
        {
            "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers/slack"},
            "id":"slack",
            "topics": ["system", "app"],
            "actions": [{
                "kind":"slack",
                "options":{
                    "channel":"#alerts"
                }
            }]
        },
        {
            "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers/smtp"},
            "id":"smtp",
            "topics": ["system", "app"],
            "actions": [{
                "kind":"smtp"
            }]
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

	topicHandlers, err := c.ListTopicHandlers(c.TopicHandlersLink("system"))
	if err != nil {
		t.Fatal(err)
	}
	exp := client.TopicHandlers{
		Link:  client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/topics/system/handlers"},
		Topic: "system",
		Handlers: []client.Handler{
			{
				ID:     "slack",
				Link:   client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/handlers/slack"},
				Topics: []string{"system", "app"},
				Actions: []client.HandlerAction{{
					Kind: "slack",
					Options: map[string]interface{}{
						"channel": "#alerts",
					},
				}},
			},
			{
				ID:     "smtp",
				Link:   client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/handlers/smtp"},
				Topics: []string{"system", "app"},
				Actions: []client.HandlerAction{{
					Kind: "smtp",
				}},
			},
		},
	}
	if !reflect.DeepEqual(exp, topicHandlers) {
		t.Errorf("unexpected  topic handlers result:\ngot:\n%v\nexp:\n%v", topicHandlers, exp)
	}
}
func Test_Handler(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() == "/kapacitor/v1preview/alerts/handlers/slack" &&
			r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
    "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers/slack"},
    "id":"slack",
    "topics": ["system", "app"],
    "actions": [{
        "kind":"slack",
        "options": {
            "channel":"#alerts"
        }
    }]
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

	h, err := c.Handler(c.HandlerLink("slack"))
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Handler{
		ID:     "slack",
		Link:   client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/handlers/slack"},
		Topics: []string{"system", "app"},
		Actions: []client.HandlerAction{{
			Kind: "slack",
			Options: map[string]interface{}{
				"channel": "#alerts",
			},
		}},
	}
	if !reflect.DeepEqual(exp, h) {
		t.Errorf("unexpected handler result:\ngot:\n%v\nexp:\n%v", h, exp)
	}
}
func Test_CreateHandler(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		options := client.HandlerOptions{}
		json.NewDecoder(r.Body).Decode(&options)
		expOptions := client.HandlerOptions{
			ID:     "slack",
			Topics: []string{"system", "app"},
			Actions: []client.HandlerAction{{
				Kind: "slack",
				Options: map[string]interface{}{
					"channel": "#alerts",
				},
			}},
		}
		if r.URL.String() == "/kapacitor/v1preview/alerts/handlers" &&
			r.Method == "POST" &&
			reflect.DeepEqual(expOptions, options) {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
    "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers/slack"},
    "id": "slack",
    "topics": ["system", "app"],
    "actions": [{
        "kind":"slack",
        "options": {
            "channel":"#alerts"
        }
    }]
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

	h, err := c.CreateHandler(client.HandlerOptions{
		ID:     "slack",
		Topics: []string{"system", "app"},
		Actions: []client.HandlerAction{{
			Kind: "slack",
			Options: map[string]interface{}{
				"channel": "#alerts",
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Handler{
		ID:     "slack",
		Link:   client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/handlers/slack"},
		Topics: []string{"system", "app"},
		Actions: []client.HandlerAction{{
			Kind: "slack",
			Options: map[string]interface{}{
				"channel": "#alerts",
			},
		}},
	}
	if !reflect.DeepEqual(exp, h) {
		t.Errorf("unexpected create handler result:\ngot:\n%v\nexp:\n%v", h, exp)
	}
}
func Test_PatchHandler(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var patch client.JSONPatch
		json.NewDecoder(r.Body).Decode(&patch)
		expPatch := client.JSONPatch{
			client.JSONOperation{
				Operation: "replace",
				Path:      "/topics",
				Value:     []interface{}{"system", "test"},
			},
			client.JSONOperation{
				Operation: "replace",
				Path:      "/actions/0/options/channel",
				Value:     "#testing_alerts",
			},
		}
		if r.URL.String() == "/kapacitor/v1preview/alerts/handlers/slack" &&
			r.Method == "PATCH" &&
			reflect.DeepEqual(expPatch, patch) {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
    "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers/slack"},
    "id": "slack",
    "topics": ["system", "test"],
    "actions": [{
        "kind":"slack",
        "options": {
            "channel":"#testing_alerts"
        }
    }]
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

	h, err := c.PatchHandler(c.HandlerLink("slack"), client.JSONPatch{
		client.JSONOperation{
			Operation: "replace",
			Path:      "/topics",
			Value:     []string{"system", "test"},
		},
		client.JSONOperation{
			Operation: "replace",
			Path:      "/actions/0/options/channel",
			Value:     "#testing_alerts",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Handler{
		ID:     "slack",
		Link:   client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/handlers/slack"},
		Topics: []string{"system", "test"},
		Actions: []client.HandlerAction{{
			Kind: "slack",
			Options: map[string]interface{}{
				"channel": "#testing_alerts",
			},
		}},
	}
	if !reflect.DeepEqual(exp, h) {
		t.Errorf("unexpected replace handler result:\ngot:\n%v\nexp:\n%v", h, exp)
	}
}
func Test_ReplaceHandler(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		options := client.HandlerOptions{}
		json.NewDecoder(r.Body).Decode(&options)
		expOptions := client.HandlerOptions{
			ID:     "slack",
			Topics: []string{"system", "test"},
			Actions: []client.HandlerAction{{
				Kind: "slack",
				Options: map[string]interface{}{
					"channel": "#testing_alerts",
				},
			}},
		}
		if r.URL.String() == "/kapacitor/v1preview/alerts/handlers/slack" &&
			r.Method == "PUT" &&
			reflect.DeepEqual(expOptions, options) {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
    "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers/slack"},
    "id": "slack",
    "topics": ["system", "test"],
    "actions": [{
        "kind":"slack",
        "options": {
            "channel":"#testing_alerts"
        }
    }]
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

	h, err := c.ReplaceHandler(c.HandlerLink("slack"), client.HandlerOptions{
		ID:     "slack",
		Topics: []string{"system", "test"},
		Actions: []client.HandlerAction{{
			Kind: "slack",
			Options: map[string]interface{}{
				"channel": "#testing_alerts",
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Handler{
		ID:     "slack",
		Link:   client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/handlers/slack"},
		Topics: []string{"system", "test"},
		Actions: []client.HandlerAction{{
			Kind: "slack",
			Options: map[string]interface{}{
				"channel": "#testing_alerts",
			},
		}},
	}
	if !reflect.DeepEqual(exp, h) {
		t.Errorf("unexpected replace handler result:\ngot:\n%v\nexp:\n%v", h, exp)
	}
}
func Test_DeleteHandler(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() == "/kapacitor/v1preview/alerts/handlers/slack" &&
			r.Method == "DELETE" {
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

	err = c.DeleteHandler(c.HandlerLink("slack"))
	if err != nil {
		t.Fatal(err)
	}
}

func Test_ListHandlers(t *testing.T) {
	s, c, err := newClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() == "/kapacitor/v1preview/alerts/handlers?pattern=%2A" &&
			r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{
    "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers"},
    "handlers": [
        {
            "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers/slack"},
            "id":"slack",
            "topics": ["system", "app"],
            "actions": [{
                "kind":"slack",
                "options": {
                    "channel":"#alerts"
                }
            }]
        },
        {
            "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers/smtp"},
            "id":"smtp",
            "topics": ["system", "app"],
            "actions": [{
                "kind":"smtp"
            }]
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

	handlers, err := c.ListHandlers(&client.ListHandlersOptions{
		Pattern: "*",
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := client.Handlers{
		Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/handlers"},
		Handlers: []client.Handler{
			{
				ID:     "slack",
				Link:   client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/handlers/slack"},
				Topics: []string{"system", "app"},
				Actions: []client.HandlerAction{{
					Kind: "slack",
					Options: map[string]interface{}{
						"channel": "#alerts",
					},
				}},
			},
			{
				ID:     "smtp",
				Link:   client.Link{Relation: client.Self, Href: "/kapacitor/v1preview/alerts/handlers/smtp"},
				Topics: []string{"system", "app"},
				Actions: []client.HandlerAction{{
					Kind: "smtp",
				}},
			},
		},
	}
	if !reflect.DeepEqual(exp, handlers) {
		t.Errorf("unexpected list handlers result:\ngot:\n%v\nexp:\n%v", handlers, exp)
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
