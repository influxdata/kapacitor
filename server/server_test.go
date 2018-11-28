package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/mail"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/dgrijalva/jwt-go"
	"github.com/google/go-cmp/cmp"
	iclient "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	imodels "github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/toml"
	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/command/commandtest"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/server"
	"github.com/influxdata/kapacitor/services/alert/alerttest"
	"github.com/influxdata/kapacitor/services/alerta/alertatest"
	"github.com/influxdata/kapacitor/services/hipchat/hipchattest"
	"github.com/influxdata/kapacitor/services/httppost"
	"github.com/influxdata/kapacitor/services/httppost/httpposttest"
	"github.com/influxdata/kapacitor/services/k8s"
	"github.com/influxdata/kapacitor/services/kafka"
	"github.com/influxdata/kapacitor/services/kafka/kafkatest"
	"github.com/influxdata/kapacitor/services/mqtt"
	"github.com/influxdata/kapacitor/services/mqtt/mqtttest"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/opsgenie/opsgenietest"
	"github.com/influxdata/kapacitor/services/opsgenie2/opsgenie2test"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/pagerduty/pagerdutytest"
	"github.com/influxdata/kapacitor/services/pagerduty2"
	"github.com/influxdata/kapacitor/services/pagerduty2/pagerduty2test"
	"github.com/influxdata/kapacitor/services/pushover/pushovertest"
	"github.com/influxdata/kapacitor/services/sensu/sensutest"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/slack/slacktest"
	"github.com/influxdata/kapacitor/services/smtp/smtptest"
	"github.com/influxdata/kapacitor/services/snmptrap/snmptraptest"
	"github.com/influxdata/kapacitor/services/swarm"
	"github.com/influxdata/kapacitor/services/talk/talktest"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/telegram/telegramtest"
	"github.com/influxdata/kapacitor/services/udf"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/influxdata/kapacitor/services/victorops/victoropstest"
	"github.com/k-sone/snmpgo"
	"github.com/pkg/errors"
)

var udfDir string

func init() {
	dir, _ := os.Getwd()
	udfDir = filepath.Clean(filepath.Join(dir, "../udf"))
}

func TestServer_Ping(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()
	_, version, err := cli.Ping()
	if err != nil {
		t.Fatal(err)
	}
	if version != "testServer" {
		t.Fatal("unexpected version", version)
	}
}

func TestServer_Pprof_Index(t *testing.T) {
	s, _ := OpenDefaultServer()
	defer s.Close()
	testCases := []struct {
		path        string
		code        int
		contentType string
	}{
		{
			path:        "/debug/pprof/",
			code:        http.StatusOK,
			contentType: "text/html; charset=utf-8",
		},
		{
			path:        "/debug/pprof/block",
			code:        http.StatusOK,
			contentType: "application/octet-stream",
		},
		{
			path:        "/debug/pprof/goroutine",
			code:        http.StatusOK,
			contentType: "application/octet-stream",
		},
		{
			path:        "/debug/pprof/heap",
			code:        http.StatusOK,
			contentType: "application/octet-stream",
		},
		{
			path:        "/debug/pprof/threadcreate",
			code:        http.StatusOK,
			contentType: "application/octet-stream",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.path, func(t *testing.T) {
			r, err := http.Get(s.URL() + tc.path)
			if err != nil {
				t.Fatal(err)
			}
			if got, exp := r.StatusCode, tc.code; got != exp {
				t.Errorf("unexpected status code got %d exp %d", got, exp)
			}
			if got, exp := r.Header.Get("Content-Type"), tc.contentType; got != exp {
				t.Errorf("unexpected content type got %s exp %s", got, exp)
			}
		})
	}
}
func TestServer_Authenticate_Fail(t *testing.T) {
	conf := NewConfig()
	conf.HTTP.AuthEnabled = true
	s := OpenServer(conf)
	cli, err := client.New(client.Config{
		URL: s.URL(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, _, err = cli.Ping()
	if err == nil {
		t.Error("expected authentication error")
	} else if exp, got := "unable to parse authentication credentials", err.Error(); got != exp {
		t.Errorf("unexpected error message: got %q exp %q", got, exp)
	}
}

func TestServer_Authenticate_User(t *testing.T) {
	conf := NewConfig()
	conf.HTTP.AuthEnabled = true
	s := OpenServer(conf)
	cli, err := client.New(client.Config{
		URL: s.URL(),
		Credentials: &client.Credentials{
			Method:   client.UserAuthentication,
			Username: "bob",
			Password: "bob's secure password",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, version, err := cli.Ping()
	if err != nil {
		t.Fatal(err)
	}
	if version != "testServer" {
		t.Fatal("unexpected version", version)
	}
}

func TestServer_Authenticate_Bearer_Fail(t *testing.T) {
	secret := "secret"
	// Create a new token object, specifying signing method and the claims
	// you would like it to contain.
	token := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
		"username": "bob",
		"exp":      time.Now().Add(10 * time.Second).Unix(),
	})

	// Sign and get the complete encoded token as a string using the secret
	tokenString, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}

	conf := NewConfig()
	conf.HTTP.AuthEnabled = true
	// Use a different secret so the token is invalid
	conf.HTTP.SharedSecret = secret + "extra secret"
	s := OpenServer(conf)
	cli, err := client.New(client.Config{
		URL: s.URL(),
		Credentials: &client.Credentials{
			Method: client.BearerAuthentication,
			Token:  tokenString,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, _, err = cli.Ping()
	if err == nil {
		t.Error("expected authentication error")
	} else if exp, got := "invalid token: signature is invalid", err.Error(); got != exp {
		t.Errorf("unexpected error message: got %q exp %q", got, exp)
	}
}

func TestServer_Authenticate_Bearer_Expired(t *testing.T) {
	secret := "secret"
	// Create a new token object, specifying signing method and the claims
	// you would like it to contain.
	token := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
		"username": "bob",
		"exp":      time.Now().Add(-10 * time.Second).Unix(),
	})

	// Sign and get the complete encoded token as a string using the secret
	tokenString, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}

	conf := NewConfig()
	conf.HTTP.AuthEnabled = true
	conf.HTTP.SharedSecret = secret
	s := OpenServer(conf)
	cli, err := client.New(client.Config{
		URL: s.URL(),
		Credentials: &client.Credentials{
			Method: client.BearerAuthentication,
			Token:  tokenString,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, _, err = cli.Ping()
	if err == nil {
		t.Error("expected authentication error")
	} else if exp, got := "invalid token: Token is expired", err.Error(); got != exp {
		t.Errorf("unexpected error message: got %q exp %q", got, exp)
	}
}

func TestServer_Authenticate_Bearer(t *testing.T) {
	secret := "secret"
	// Create a new token object, specifying signing method and the claims
	// you would like it to contain.
	token := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
		"username": "bob",
		"exp":      time.Now().Add(10 * time.Second).Unix(),
	})

	// Sign and get the complete encoded token as a string using the secret
	tokenString, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}

	conf := NewConfig()
	conf.HTTP.AuthEnabled = true
	conf.HTTP.SharedSecret = secret
	s := OpenServer(conf)
	cli, err := client.New(client.Config{
		URL: s.URL(),
		Credentials: &client.Credentials{
			Method: client.BearerAuthentication,
			Token:  tokenString,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
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

func TestServer_CreateTask_Quiet(t *testing.T) {
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
        .quiet()
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

func TestServer_CreateTaskImplicitStream(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTaskID"
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
	tick := `dbrp "mydb"."myrp"

dbrp "otherdb"."default"

stream
    |from()
        .measurement('test')
`
	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
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

func TestServer_CreateTaskBatch(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTaskID"
	dbrps := []client.DBRP{
		{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		},
	}
	tick := `dbrp "mydb"."myrp"

batch
    |query('SELECT * from mydb.myrp.mymeas')
    |log()
`
	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
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
	if ti.Type != client.BatchTask {
		t.Fatalf("unexpected type got %v exp %v", ti.Type, client.BatchTask)
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
	dot := "digraph testTaskID {\nquery1 -> log2;\n}"
	if ti.Dot != dot {
		t.Fatalf("unexpected dot\ngot\n%s\nexp\n%s\n", ti.Dot, dot)
	}
}

func TestServer_CreateTaskImplicitAndExplicit(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTaskID"
	dbrps := []client.DBRP{
		{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		},
	}
	tick := `dbrp "mydb"."myrp"

dbrp "otherdb"."default"

stream
    |from()
        .measurement('test')
`
	_, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	})

	// It is expected that error should be non nil
	if err == nil {
		t.Fatal("expected task to fail to be created")
	}
}

func TestServer_CreateTaskExplicitUpdateImplicit(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTaskID"
	createDBRPs := []client.DBRP{
		{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		},
		{
			Database:        "otherdb",
			RetentionPolicy: "default",
		},
	}
	createTick := `stream
    |from()
        .measurement('test')
`
	updateDBRPs := []client.DBRP{
		{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		},
	}
	updateTick := `dbrp "mydb"."myrp"

stream
    |from()
        .measurement('test')
`
	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		DBRPs:      createDBRPs,
		TICKscript: createTick,
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
	if !reflect.DeepEqual(ti.DBRPs, createDBRPs) {
		t.Fatalf("unexpected dbrps got %s exp %s", ti.DBRPs, createDBRPs)
	}
	if ti.TICKscript != createTick {
		t.Fatalf("unexpected TICKscript got %s exp %s", ti.TICKscript, createTick)
	}
	dot := "digraph testTaskID {\nstream0 -> from1;\n}"
	if ti.Dot != dot {
		t.Fatalf("unexpected dot\ngot\n%s\nexp\n%s\n", ti.Dot, dot)
	}

	_, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		TICKscript: updateTick,
	})
	if err != nil {
		t.Fatal(err)
	}

	ti, err = cli.Task(task.Link, nil)
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
	if !reflect.DeepEqual(ti.DBRPs, updateDBRPs) {
		t.Fatalf("unexpected dbrps got %s exp %s", ti.DBRPs, updateDBRPs)
	}
	if ti.TICKscript != updateTick {
		t.Fatalf("unexpected TICKscript got %s exp %s", ti.TICKscript, updateTick)
	}
	dot = "digraph testTaskID {\nstream0 -> from1;\n}"
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

	_, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
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

stream0 [avg_exec_time_ns="0s" errors="0" working_cardinality="0" ];
stream0 -> from1 [processed="0"];

from1 [avg_exec_time_ns="0s" errors="0" working_cardinality="0" ];
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

stream0 [avg_exec_time_ns="0s" errors="0" working_cardinality="0" ];
stream0 -> from1 [processed="0"];

from1 [avg_exec_time_ns="0s" errors="0" working_cardinality="0" ];
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

	_, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
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

func TestServer_TaskNums(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTaskID"
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

	// Create a bunch of tasks with every 3rd task enabled
	count := 100
	enabled := 0
	tasks := make([]client.Task, count)
	for i := 0; i < count; i++ {
		status := client.Disabled
		if i%3 == 0 {
			enabled++
			status = client.Enabled
		}
		task, err := cli.CreateTask(client.CreateTaskOptions{
			ID:         fmt.Sprintf("%s-%d", id, i),
			Type:       ttype,
			DBRPs:      dbrps,
			TICKscript: tick,
			Status:     status,
		})
		if err != nil {
			t.Fatal(err)
		}
		tasks[i] = task
	}
	if stats, err := s.Stats(); err != nil {
		t.Fatal(err)
	} else {
		if got, exp := stats.NumTasks, count; got != exp {
			t.Errorf("unexpected num_tasks got %d exp %d", got, exp)
		}
		if got, exp := stats.NumEnabledTasks, enabled; got != exp {
			t.Errorf("unexpected num_enabled_tasks got %d exp %d", got, exp)
		}
	}

	// Enable a bunch of tasks
	for i, task := range tasks {
		if i%2 == 0 && task.Status != client.Enabled {
			enabled++
			tasks[i].Status = client.Enabled
			if _, err := cli.UpdateTask(task.Link, client.UpdateTaskOptions{
				Status: client.Enabled,
			}); err != nil {
				t.Fatal(err)
			}
		}
	}

	if stats, err := s.Stats(); err != nil {
		t.Fatal(err)
	} else {
		if got, exp := stats.NumTasks, count; got != exp {
			t.Errorf("unexpected num_tasks got %d exp %d", got, exp)
		}
		if got, exp := stats.NumEnabledTasks, enabled; got != exp {
			t.Errorf("unexpected num_enabled_tasks got %d exp %d", got, exp)
		}
	}

	// Disable a bunch of tasks
	for i, task := range tasks {
		if i%5 == 0 && task.Status != client.Disabled {
			enabled--
			tasks[i].Status = client.Disabled
			if _, err := cli.UpdateTask(task.Link, client.UpdateTaskOptions{
				Status: client.Disabled,
			}); err != nil {
				t.Fatal(err)
			}
		}
	}

	if stats, err := s.Stats(); err != nil {
		t.Fatal(err)
	} else {
		if got, exp := stats.NumTasks, count; got != exp {
			t.Errorf("unexpected num_tasks got %d exp %d", got, exp)
		}
		if got, exp := stats.NumEnabledTasks, enabled; got != exp {
			t.Errorf("unexpected num_enabled_tasks got %d exp %d", got, exp)
		}
	}

	// Delete a bunch of tasks
	for i, task := range tasks {
		if i%6 == 0 {
			count--
			if task.Status == client.Enabled {
				enabled--
			}
			if err := cli.DeleteTask(task.Link); err != nil {
				t.Fatal(err)
			}
		}
	}

	if stats, err := s.Stats(); err != nil {
		t.Fatal(err)
	} else {
		if got, exp := stats.NumTasks, count; got != exp {
			t.Errorf("unexpected num_tasks got %d exp %d", got, exp)
		}
		if got, exp := stats.NumEnabledTasks, enabled; got != exp {
			t.Errorf("unexpected num_enabled_tasks got %d exp %d", got, exp)
		}
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

func TestServer_CreateTemplate(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTemplateID"
	ttype := client.StreamTask
	tick := `var x = 5

stream
    |from()
        .measurement('test')
`
	template, err := cli.CreateTemplate(client.CreateTemplateOptions{
		ID:         id,
		Type:       ttype,
		TICKscript: tick,
	})
	if err != nil {
		t.Fatal(err)
	}

	ti, err := cli.Template(template.Link, nil)
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
	if ti.TICKscript != tick {
		t.Fatalf("unexpected TICKscript got\n%s\nexp\n%s\n", ti.TICKscript, tick)
	}
	dot := "digraph testTemplateID {\nstream0 -> from1;\n}"
	if ti.Dot != dot {
		t.Fatalf("unexpected dot\ngot\n%s\nexp\n%s\n", ti.Dot, dot)
	}
	vars := client.Vars{"x": {Value: int64(5), Type: client.VarInt}}
	if !reflect.DeepEqual(vars, ti.Vars) {
		t.Fatalf("unexpected vars\ngot\n%s\nexp\n%s\n", ti.Vars, vars)
	}
}
func TestServer_UpdateTemplateID(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTemplateID"
	ttype := client.StreamTask
	tick := `var x = 5

stream
    |from()
        .measurement('test')
`
	template, err := cli.CreateTemplate(client.CreateTemplateOptions{
		ID:         id,
		Type:       ttype,
		TICKscript: tick,
	})
	if err != nil {
		t.Fatal(err)
	}

	ti, err := cli.Template(template.Link, nil)
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
	if ti.TICKscript != tick {
		t.Fatalf("unexpected TICKscript got\n%s\nexp\n%s\n", ti.TICKscript, tick)
	}
	dot := "digraph testTemplateID {\nstream0 -> from1;\n}"
	if ti.Dot != dot {
		t.Fatalf("unexpected dot\ngot\n%s\nexp\n%s\n", ti.Dot, dot)
	}
	vars := client.Vars{"x": {Value: int64(5), Type: client.VarInt}}
	if !reflect.DeepEqual(vars, ti.Vars) {
		t.Fatalf("unexpected vars\ngot\n%s\nexp\n%s\n", ti.Vars, vars)
	}

	newID := "newTemplateID"
	template, err = cli.UpdateTemplate(template.Link, client.UpdateTemplateOptions{
		ID: newID,
	})
	if err != nil {
		t.Fatal(err)
	}

	if got, exp := template.Link.Href, "/kapacitor/v1/templates/newTemplateID"; got != exp {
		t.Fatalf("unexpected template link got %s exp %s", got, exp)
	}

	ti, err = cli.Template(template.Link, nil)
	if err != nil {
		t.Fatal(err)
	}

	if ti.Error != "" {
		t.Fatal(ti.Error)
	}
	if ti.ID != newID {
		t.Fatalf("unexpected id got %s exp %s", ti.ID, newID)
	}
	if ti.Type != client.StreamTask {
		t.Fatalf("unexpected type got %v exp %v", ti.Type, client.StreamTask)
	}
	if ti.TICKscript != tick {
		t.Fatalf("unexpected TICKscript got\n%s\nexp\n%s\n", ti.TICKscript, tick)
	}
	dot = "digraph newTemplateID {\nstream0 -> from1;\n}"
	if ti.Dot != dot {
		t.Fatalf("unexpected dot\ngot\n%s\nexp\n%s\n", ti.Dot, dot)
	}
	if !reflect.DeepEqual(vars, ti.Vars) {
		t.Fatalf("unexpected vars\ngot\n%s\nexp\n%s\n", ti.Vars, vars)
	}
}

func TestServer_CreateTemplateImplicitAndUpdateExplicitWithTasks(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTemplateID"
	implicitTick := `dbrp "telegraf"."autogen"

var x = 5

stream
    |from()
        .measurement('test')
`
	template, err := cli.CreateTemplate(client.CreateTemplateOptions{
		ID:         id,
		TICKscript: implicitTick,
	})
	if err != nil {
		t.Fatal(err)
	}

	ti, err := cli.Template(template.Link, nil)
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
	if ti.TICKscript != implicitTick {
		t.Fatalf("unexpected TICKscript got\n%s\nexp\n%s\n", ti.TICKscript, implicitTick)
	}
	dot := "digraph testTemplateID {\nstream0 -> from1;\n}"
	if ti.Dot != dot {
		t.Fatalf("unexpected dot\ngot\n%s\nexp\n%s\n", ti.Dot, dot)
	}
	vars := client.Vars{"x": {Value: int64(5), Type: client.VarInt}}
	if !reflect.DeepEqual(vars, ti.Vars) {
		t.Fatalf("unexpected vars\ngot\n%s\nexp\n%s\n", ti.Vars, vars)
	}

	implicitDBRPs := []client.DBRP{
		{
			Database:        "telegraf",
			RetentionPolicy: "autogen",
		},
	}

	count := 1
	tasks := make([]client.Task, count)
	for i := 0; i < count; i++ {
		task, err := cli.CreateTask(client.CreateTaskOptions{
			TemplateID: template.ID,
			Status:     client.Enabled,
		})
		if err != nil {
			t.Fatal(err)
		}
		tasks[i] = task

		ti, err := cli.Task(task.Link, nil)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(ti.DBRPs, implicitDBRPs) {
			t.Fatalf("unexpected dbrps got %s exp %s", ti.DBRPs, implicitDBRPs)
		}
	}

	updateTick := `var x = 5

	stream
	    |from()
	        .measurement('test')
	`

	_, err = cli.UpdateTemplate(template.Link, client.UpdateTemplateOptions{
		ID:         id,
		TICKscript: updateTick,
	})
	// Expects error
	if err == nil {
		t.Fatal(err)
	}

	finalTick := `dbrp "telegraf"."autogen"

	dbrp "telegraf"."not_autogen"

	var x = 5

	stream
	    |from()
	        .measurement('test')
	`

	finalDBRPs := []client.DBRP{
		{
			Database:        "telegraf",
			RetentionPolicy: "autogen",
		},
		{
			Database:        "telegraf",
			RetentionPolicy: "not_autogen",
		},
	}
	template, err = cli.UpdateTemplate(template.Link, client.UpdateTemplateOptions{
		ID:         id,
		TICKscript: finalTick,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, task := range tasks {
		ti, err := cli.Task(task.Link, nil)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(ti.DBRPs, finalDBRPs) {
			t.Fatalf("unexpected dbrps got %s exp %s", ti.DBRPs, finalDBRPs)
		}
	}
}
func TestServer_UpdateTemplateID_WithTasks(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTemplateID"
	ttype := client.StreamTask
	tick := `var x = 5

stream
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

	template, err := cli.CreateTemplate(client.CreateTemplateOptions{
		ID:         id,
		Type:       ttype,
		TICKscript: tick,
	})
	if err != nil {
		t.Fatal(err)
	}

	count := 100
	tasks := make([]client.Task, count)
	for i := 0; i < count; i++ {
		task, err := cli.CreateTask(client.CreateTaskOptions{
			TemplateID: template.ID,
			DBRPs:      dbrps,
			Status:     client.Enabled,
		})
		if err != nil {
			t.Fatal(err)
		}
		tasks[i] = task
	}

	newID := "newTemplateID"
	template, err = cli.UpdateTemplate(template.Link, client.UpdateTemplateOptions{
		ID: newID,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, task := range tasks {
		got, err := cli.Task(task.Link, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got.TemplateID != newID {
			t.Errorf("unexpected task TemplateID got %s exp %s", got.TemplateID, newID)
		}
		if got.TICKscript != tick {
			t.Errorf("unexpected task TICKscript got %s exp %s", got.TICKscript, tick)
		}
	}
}
func TestServer_UpdateTemplateID_Fail(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTemplateID"
	newID := "anotherTemplateID"
	ttype := client.StreamTask
	tick := `var x = 5

stream
    |from()
        .measurement('test')
`
	template, err := cli.CreateTemplate(client.CreateTemplateOptions{
		ID:         id,
		Type:       ttype,
		TICKscript: tick,
	})
	if err != nil {
		t.Fatal(err)
	}

	ti, err := cli.Template(template.Link, nil)
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
	if ti.TICKscript != tick {
		t.Fatalf("unexpected TICKscript got\n%s\nexp\n%s\n", ti.TICKscript, tick)
	}
	dot := "digraph testTemplateID {\nstream0 -> from1;\n}"
	if ti.Dot != dot {
		t.Fatalf("unexpected dot\ngot\n%s\nexp\n%s\n", ti.Dot, dot)
	}
	vars := client.Vars{"x": {Value: int64(5), Type: client.VarInt}}
	if !reflect.DeepEqual(vars, ti.Vars) {
		t.Fatalf("unexpected vars\ngot\n%s\nexp\n%s\n", ti.Vars, vars)
	}

	// Create conflicting template
	if _, err := cli.CreateTemplate(client.CreateTemplateOptions{
		ID:         newID,
		Type:       ttype,
		TICKscript: tick,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = cli.UpdateTemplate(template.Link, client.UpdateTemplateOptions{
		ID: newID,
	}); err == nil {
		t.Fatal("expected update template to fail on name conflict")
	}

	// Can still get old template
	ti, err = cli.Template(template.Link, nil)
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
	if ti.TICKscript != tick {
		t.Fatalf("unexpected TICKscript got\n%s\nexp\n%s\n", ti.TICKscript, tick)
	}
	if ti.Dot != dot {
		t.Fatalf("unexpected dot\ngot\n%s\nexp\n%s\n", ti.Dot, dot)
	}
	if !reflect.DeepEqual(vars, ti.Vars) {
		t.Fatalf("unexpected vars\ngot\n%s\nexp\n%s\n", ti.Vars, vars)
	}
}
func TestServer_UpdateTemplateID_WithTasks_Fail(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTemplateID"
	ttype := client.StreamTask
	tick := `var x = 5

stream
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

	template, err := cli.CreateTemplate(client.CreateTemplateOptions{
		ID:         id,
		Type:       ttype,
		TICKscript: tick,
	})
	if err != nil {
		t.Fatal(err)
	}

	count := 100
	tasks := make([]client.Task, count)
	for i := 0; i < count; i++ {
		task, err := cli.CreateTask(client.CreateTaskOptions{
			TemplateID: template.ID,
			DBRPs:      dbrps,
			Status:     client.Enabled,
		})
		if err != nil {
			t.Fatal(err)
		}
		tasks[i] = task
	}

	// Create conflicting template
	newID := "newTemplateID"
	if _, err := cli.CreateTemplate(client.CreateTemplateOptions{
		ID:         newID,
		Type:       ttype,
		TICKscript: tick,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = cli.UpdateTemplate(template.Link, client.UpdateTemplateOptions{
		ID:         newID,
		TICKscript: "stream",
	}); err == nil {
		t.Fatal("expected update template to fail on conflicting name")
	}

	for _, task := range tasks {
		got, err := cli.Task(task.Link, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got.TemplateID != id {
			t.Errorf("unexpected task TemplateID got %s exp %s", got.TemplateID, id)
		}
		if got.TICKscript != tick {
			t.Errorf("unexpected task TICKscript got %s exp %s", got.TICKscript, tick)
		}
	}
}

func TestServer_DeleteTemplate(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTemplateID"
	ttype := client.StreamTask
	tick := `stream
    |from()
        .measurement('test')
`
	template, err := cli.CreateTemplate(client.CreateTemplateOptions{
		ID:         id,
		Type:       ttype,
		TICKscript: tick,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cli.DeleteTemplate(template.Link)
	if err != nil {
		t.Fatal(err)
	}

	ti, err := cli.Template(template.Link, nil)
	if err == nil {
		t.Fatal("unexpected template:", ti)
	}
}

func TestServer_CreateTaskFromTemplate(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTemplateID"
	ttype := client.StreamTask
	tick := `// Configurable measurement
var measurement = 'test'

stream
    |from()
        .measurement(measurement)
`
	template, err := cli.CreateTemplate(client.CreateTemplateOptions{
		ID:         id,
		Type:       ttype,
		TICKscript: tick,
	})
	if err != nil {
		t.Fatal(err)
	}

	templateInfo, err := cli.Template(template.Link, nil)
	if err != nil {
		t.Fatal(err)
	}

	if templateInfo.Error != "" {
		t.Fatal(templateInfo.Error)
	}
	if templateInfo.ID != id {
		t.Fatalf("unexpected template.id got %s exp %s", templateInfo.ID, id)
	}
	if templateInfo.Type != client.StreamTask {
		t.Fatalf("unexpected template.type got %v exp %v", templateInfo.Type, client.StreamTask)
	}
	if templateInfo.TICKscript != tick {
		t.Fatalf("unexpected template.TICKscript got %s exp %s", templateInfo.TICKscript, tick)
	}
	dot := "digraph testTemplateID {\nstream0 -> from1;\n}"
	if templateInfo.Dot != dot {
		t.Fatalf("unexpected template.dot\ngot\n%s\nexp\n%s\n", templateInfo.Dot, dot)
	}
	expVars := client.Vars{
		"measurement": {
			Value:       "test",
			Type:        client.VarString,
			Description: "Configurable measurement",
		},
	}
	if got, exp := templateInfo.Vars, expVars; !reflect.DeepEqual(exp, got) {
		t.Errorf("unexpected template vars: got %v exp %v", got, exp)
	}

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
	vars := client.Vars{
		"measurement": {
			Value: "another_measurement",
			Type:  client.VarString,
		},
	}

	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         "taskid",
		TemplateID: id,
		DBRPs:      dbrps,
		Vars:       vars,
	})
	if err != nil {
		t.Fatal(err)
	}

	taskInfo, err := cli.Task(task.Link, nil)
	if err != nil {
		t.Fatal(err)
	}

	if taskInfo.Error != "" {
		t.Fatal(taskInfo.Error)
	}
	if taskInfo.ID != "taskid" {
		t.Fatalf("unexpected task.id got %s exp %s", taskInfo.ID, "taskid")
	}
	if taskInfo.Type != client.StreamTask {
		t.Fatalf("unexpected task.type got %v exp %v", taskInfo.Type, client.StreamTask)
	}
	if taskInfo.TICKscript != tick {
		t.Fatalf("unexpected task.TICKscript got %s exp %s", taskInfo.TICKscript, tick)
	}
	dot = "digraph taskid {\nstream0 -> from1;\n}"
	if taskInfo.Dot != dot {
		t.Fatalf("unexpected task.dot\ngot\n%s\nexp\n%s\n", taskInfo.Dot, dot)
	}
	if taskInfo.Status != client.Disabled {
		t.Fatalf("unexpected task.status got %v exp %v", taskInfo.Status, client.Disabled)
	}
	if !reflect.DeepEqual(taskInfo.DBRPs, dbrps) {
		t.Fatalf("unexpected task.dbrps got %s exp %s", taskInfo.DBRPs, dbrps)
	}
	if !reflect.DeepEqual(taskInfo.Vars, vars) {
		t.Fatalf("unexpected task.vars got %s exp %s", taskInfo.Vars, vars)
	}
}

func TestServer_DynamicStreamTask(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	testCases := []struct {
		name string
		tick string
		want client.TaskType
	}{
		{
			name: "stream",
			tick: `
dbrp "db"."rp"
stream
    |from()
         .measurement('test')
`,
			want: client.StreamTask,
		},
		{
			name: "stream_through_var",
			tick: `
dbrp "db"."rp"
var s = stream
s
    |from()
         .measurement('test')
`,
			want: client.StreamTask,
		},
		{
			name: "batch",
			tick: `
dbrp "db"."rp"
batch
    |query('select * from db.rp.m')
`,
			want: client.BatchTask,
		},
		{
			name: "batch_through_var",
			tick: `
dbrp "db"."rp"
var b = batch
b
    |query('select * from db.rp.m')
`,
			want: client.BatchTask,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			task, err := cli.CreateTask(client.CreateTaskOptions{
				ID:         tc.name,
				TICKscript: tc.tick,
				Status:     client.Disabled,
			})
			if err != nil {
				t.Fatal(err)
			}

			if task.Type != tc.want {
				t.Fatalf("unexpected task type: got: %v want: %v", task.Type, tc.want)
			}
		})
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

	_, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), id)

	// Request data before any writes and expect null responses
	nullResponse := `{"series":null}`
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

func TestServer_StreamTask_NoRP(t *testing.T) {
	conf := NewConfig()
	conf.DefaultRetentionPolicy = "myrp"
	s := OpenServer(conf)
	defer s.Close()
	cli := Client(s)

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

	_, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), id)

	// Request data before any writes and expect null responses
	nullResponse := `{"series":null}`
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
	s.MustWrite("mydb", "", points, v)

	exp := `{"series":[{"name":"test","columns":["time","count"],"values":[["1970-01-01T00:00:10Z",15]]}]}`
	err = s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5)
	if err != nil {
		t.Error(err)
	}
}

func TestServer_StreamTemplateTask(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	templateId := "testStreamTemplate"
	taskId := "testStreamTask"
	ttype := client.StreamTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	tick := `
var field = 'nonexistent'
stream
    |from()
        .measurement('test')
    |window()
        .period(10s)
        .every(10s)
    |count(field)
    |httpOut('count')
`
	if _, err := cli.CreateTemplate(client.CreateTemplateOptions{
		ID:         templateId,
		Type:       ttype,
		TICKscript: tick,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         taskId,
		TemplateID: templateId,
		DBRPs:      dbrps,
		Status:     client.Enabled,
		Vars: client.Vars{
			"field": {
				Value: "value",
				Type:  client.VarString,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), taskId)

	// Request data before any writes and expect null responses
	nullResponse := `{"series":null}`
	if err := s.HTTPGetRetry(endpoint, nullResponse, 100, time.Millisecond*5); err != nil {
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
	if err := s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5); err != nil {
		t.Error(err)
	}
}
func TestServer_StreamTemplateTask_MissingVar(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	templateId := "testStreamTemplate"
	taskId := "testStreamTask"
	ttype := client.StreamTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	tick := `
var field string
stream
    |from()
        .measurement('test')
    |window()
        .period(10s)
        .every(10s)
    |count(field)
    |httpOut('count')
`
	if _, err := cli.CreateTemplate(client.CreateTemplateOptions{
		ID:         templateId,
		Type:       ttype,
		TICKscript: tick,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         taskId,
		TemplateID: templateId,
		DBRPs:      dbrps,
		Status:     client.Enabled,
	}); err == nil {
		t.Error("expected error for missing task vars")
	} else if exp, got := "invalid TICKscript: missing value for var \"field\".", err.Error(); got != exp {
		t.Errorf("unexpected error message: got %s exp %s", got, exp)
	}
}
func TestServer_StreamTemplateTask_AllTypes(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	templateId := "testStreamTemplate"
	taskId := "testStreamTask"
	ttype := client.StreamTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	tick := `
var bool bool
var count_threshold int
var value_threshold float
var window duration
var field string
var tagMatch regex
var match lambda
var eval lambda
var groups list
var secondGroup list
stream
    |from()
        .measurement('test')
        .where(lambda: match AND "tag" =~ tagMatch AND bool AND "value" >= value_threshold)
        .groupBy(groups)
        |log().prefix('FROM')
    |window()
        .period(window)
        .every(window)
        |log().prefix('WINDOW')
    |count(field)
        |log().prefix('COUNT')
    |groupBy(secondGroup)
    |sum('count')
        .as('count')
        |log().prefix('SUM')
    |where(lambda: "count" >= count_threshold)
        |log().prefix('WHERE')
    |eval(eval)
        .as('count')
    |httpOut('count')
`
	if _, err := cli.CreateTemplate(client.CreateTemplateOptions{
		ID:         templateId,
		Type:       ttype,
		TICKscript: tick,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         taskId,
		TemplateID: templateId,
		DBRPs:      dbrps,
		Status:     client.Enabled,
		Vars: client.Vars{
			"bool": {
				Value: true,
				Type:  client.VarBool,
			},
			"count_threshold": {
				Value: int64(1),
				Type:  client.VarInt,
			},
			"value_threshold": {
				Value: float64(1.0),
				Type:  client.VarFloat,
			},
			"window": {
				Value: 10 * time.Second,
				Type:  client.VarDuration,
			},
			"field": {
				Value: "value",
				Type:  client.VarString,
			},
			"tagMatch": {
				Value: "^a.*",
				Type:  client.VarRegex,
			},
			"match": {
				Value: `"value" == 1.0`,
				Type:  client.VarLambda,
			},
			"eval": {
				Value: `"count" * 2`,
				Type:  client.VarLambda,
			},
			"groups": {
				Value: []client.Var{client.Var{Type: client.VarStar}},
				Type:  client.VarList,
			},
			"secondGroup": {
				Value: []client.Var{client.Var{Value: "tag", Type: client.VarString}},
				Type:  client.VarList,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), taskId)

	// Request data before any writes and expect null responses
	nullResponse := `{"series":null}`
	if err := s.HTTPGetRetry(endpoint, nullResponse, 100, time.Millisecond*5); err != nil {
		t.Error(err)
	}

	points := `test,tag=abc,other=a value=1 0000000000
test,tag=abc,other=b value=1 0000000000
test,tag=abc,other=a value=1 0000000001
test,tag=bbc,other=b value=1 0000000001
test,tag=abc,other=a value=1 0000000002
test,tag=abc,other=a value=0 0000000002
test,tag=abc,other=b value=1 0000000003
test,tag=abc,other=a value=1 0000000003
test,tag=abc,other=a value=1 0000000004
test,tag=abc,other=b value=1 0000000005
test,tag=abc,other=a value=1 0000000005
test,tag=bbc,other=a value=1 0000000005
test,tag=abc,other=b value=1 0000000006
test,tag=abc,other=a value=1 0000000007
test,tag=abc,other=b value=0 0000000008
test,tag=abc,other=a value=1 0000000009
test,tag=abc,other=a value=1 0000000010
test,tag=abc,other=a value=1 0000000011
test,tag=abc,other=b value=1 0000000011
test,tag=bbc,other=a value=1 0000000011
test,tag=bbc,other=b value=1 0000000011
test,tag=abc,other=a value=1 0000000021
`
	v := url.Values{}
	v.Add("precision", "s")
	s.MustWrite("mydb", "myrp", points, v)

	exp := `{"series":[{"name":"test","tags":{"tag":"abc"},"columns":["time","count"],"values":[["1970-01-01T00:00:10Z",24]]}]}`
	if err := s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5); err != nil {
		t.Error(err)
	}
}

func TestServer_StreamTemplateTaskFromUpdate(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	templateId := "testStreamTemplate"
	taskId := "testStreamTask"
	ttype := client.StreamTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	tick := `
var field = 'nonexistent'
stream
    |from()
        .measurement('test')
    |window()
        .period(10s)
        .every(10s)
    |count(field)
    |httpOut('count')
`
	if _, err := cli.CreateTemplate(client.CreateTemplateOptions{
		ID:         templateId,
		Type:       ttype,
		TICKscript: tick,
	}); err != nil {
		t.Fatal(err)
	}

	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         taskId,
		TemplateID: templateId,
		DBRPs:      dbrps,
		Status:     client.Disabled,
		Vars: client.Vars{
			"field": {
				Value: "value",
				Type:  client.VarString,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	}); err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), taskId)

	// Request data before any writes and expect null responses
	nullResponse := `{"series":null}`
	if err := s.HTTPGetRetry(endpoint, nullResponse, 100, time.Millisecond*5); err != nil {
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
	if err := s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5); err != nil {
		t.Error(err)
	}
}
func TestServer_StreamTemplateTask_UpdateTemplate(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	templateId := "testStreamTemplate"
	taskId := "testStreamTask"
	ttype := client.StreamTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	tickWrong := `
stream
    |from()
        .measurement('test')
    |window()
        .period(10s)
        .every(10s)
    |count('wrong')
    |httpOut('count')
`
	tickCorrect := `
var field string
stream
    |from()
        .measurement('test')
    |window()
        .period(10s)
        .every(10s)
    |count(field)
    |httpOut('count')
`
	template, err := cli.CreateTemplate(client.CreateTemplateOptions{
		ID:         templateId,
		Type:       ttype,
		TICKscript: tickWrong,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err = cli.CreateTask(client.CreateTaskOptions{
		ID:         taskId,
		TemplateID: templateId,
		DBRPs:      dbrps,
		Status:     client.Enabled,
		Vars: client.Vars{
			"field": {
				Value: "value",
				Type:  client.VarString,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := cli.UpdateTemplate(template.Link, client.UpdateTemplateOptions{
		TICKscript: tickCorrect,
	}); err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), taskId)

	// Request data before any writes and expect null responses
	nullResponse := `{"series":null}`
	if err := s.HTTPGetRetry(endpoint, nullResponse, 100, time.Millisecond*5); err != nil {
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
	if err := s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5); err != nil {
		t.Error(err)
	}
}
func TestServer_StreamTemplateTask_UpdateTemplate_Rollback(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	templateId := "testStreamTemplate"
	taskId := "testStreamTask"
	ttype := client.StreamTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	tickCorrect := `
var field string
stream
    |from()
        .measurement('test')
    |window()
        .period(10s)
        .every(10s)
    |count(field)
    |httpOut('count')
`
	tickNewVar := `
var field string
var period duration
stream
    |from()
        .measurement('test')
    |window()
        .period(period)
        .every(period)
    |count(field)
    |httpOut('count')
`
	template, err := cli.CreateTemplate(client.CreateTemplateOptions{
		ID:         templateId,
		Type:       ttype,
		TICKscript: tickCorrect,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create several tasks
	count := 5
	tasks := make([]client.Task, count)
	for i := 0; i < count; i++ {
		if task, err := cli.CreateTask(client.CreateTaskOptions{
			ID:         fmt.Sprintf("%s-%d", taskId, i),
			TemplateID: templateId,
			DBRPs:      dbrps,
			Status:     client.Enabled,
			Vars: client.Vars{
				"field": {
					Value: "value",
					Type:  client.VarString,
				},
			},
		}); err != nil {
			t.Fatal(err)
		} else {
			tasks[i] = task
		}
	}

	if _, err := cli.UpdateTemplate(template.Link, client.UpdateTemplateOptions{
		TICKscript: tickNewVar,
	}); err == nil {
		t.Error("expected error for breaking template update, got nil")
	} else if got, exp := err.Error(), `error reloading associated task testStreamTask-0: missing value for var "period".`; exp != got {
		t.Errorf("unexpected error for breaking template update, got %s exp %s", got, exp)
	}

	// Get all tasks and make sure their TICKscript has the original value
	for _, task := range tasks {
		if gotTask, err := cli.Task(task.Link, &client.TaskOptions{ScriptFormat: "raw"}); err != nil {
			t.Fatal(err)
		} else if got, exp := gotTask.TICKscript, tickCorrect; got != exp {
			t.Errorf("unexpected task TICKscript:\ngot\n%s\nexp\n%s\n", got, exp)
		}
	}

	// Update all tasks with new var
	for _, task := range tasks {
		if _, err := cli.UpdateTask(task.Link, client.UpdateTaskOptions{
			Vars: client.Vars{
				"field": {
					Value: "value",
					Type:  client.VarString,
				},
				"period": {
					Value: 10 * time.Second,
					Type:  client.VarDuration,
				},
			},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Now update template should succeed since the tasks are updated too.
	if _, err := cli.UpdateTemplate(template.Link, client.UpdateTemplateOptions{
		TICKscript: tickNewVar,
	}); err != nil {
		t.Fatal(err)
	}

	for _, task := range tasks {
		taskId := task.ID
		endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), taskId)

		// Request data before any writes and expect null responses
		nullResponse := `{"series":null}`
		if err := s.HTTPGetRetry(endpoint, nullResponse, 100, time.Millisecond*5); err != nil {
			t.Error(err)
		}
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

	for _, task := range tasks {
		taskId := task.ID
		endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), taskId)

		exp := `{"series":[{"name":"test","columns":["time","count"],"values":[["1970-01-01T00:00:10Z",15]]}]}`
		if err := s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5); err != nil {
			t.Error(err)
		}
	}
}

func TestServer_UpdateTaskID(t *testing.T) {
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

	newID := "newTaskID"
	task, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		ID: newID,
	})
	if err != nil {
		t.Fatal(err)
	}

	if got, exp := task.Link.Href, "/kapacitor/v1/tasks/newTaskID"; got != exp {
		t.Fatalf("unexpected task link got %s exp %s", got, exp)
	}

	ti, err = cli.Task(task.Link, nil)
	if err != nil {
		t.Fatal(err)
	}

	if ti.Error != "" {
		t.Fatal(ti.Error)
	}
	if ti.ID != newID {
		t.Fatalf("unexpected id got %s exp %s", ti.ID, newID)
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
	dot = "digraph newTaskID {\nstream0 -> from1;\n}"
	if ti.Dot != dot {
		t.Fatalf("unexpected dot\ngot\n%s\nexp\n%s\n", ti.Dot, dot)
	}
}
func TestServer_UpdateTaskID_Fail(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testTaskID"
	newID := "anotherTaskID"
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

	// Create conflicting task
	if _, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         newID,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		ID: newID,
	}); err == nil {
		t.Fatal("expected error on name conflict")
	}

	// Can still get old task
	ti, err = cli.Task(task.Link, nil)
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
	if ti.Dot != dot {
		t.Fatalf("unexpected dot\ngot\n%s\nexp\n%s\n", ti.Dot, dot)
	}
}
func TestServer_UpdateTaskID_Enabled(t *testing.T) {
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
	if !reflect.DeepEqual(ti.DBRPs, dbrps) {
		t.Fatalf("unexpected dbrps got %s exp %s", ti.DBRPs, dbrps)
	}
	if ti.TICKscript != tick {
		t.Fatalf("unexpected TICKscript got %s exp %s", ti.TICKscript, tick)
	}
	if !ti.Executing {
		t.Fatal("expected task to be executing")
	}

	newID := "newTaskID"
	task, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		ID: newID,
	})
	if err != nil {
		t.Fatal(err)
	}

	if got, exp := task.Link.Href, "/kapacitor/v1/tasks/newTaskID"; got != exp {
		t.Fatalf("unexpected task link got %s exp %s", got, exp)
	}

	ti, err = cli.Task(task.Link, nil)
	if err != nil {
		t.Fatal(err)
	}

	if ti.Error != "" {
		t.Fatal(ti.Error)
	}
	if ti.ID != newID {
		t.Fatalf("unexpected id got %s exp %s", ti.ID, newID)
	}
	if ti.Type != client.StreamTask {
		t.Fatalf("unexpected type got %v exp %v", ti.Type, client.StreamTask)
	}
	if ti.Status != client.Enabled {
		t.Fatalf("unexpected status got %v exp %v", ti.Status, client.Enabled)
	}
	if !reflect.DeepEqual(ti.DBRPs, dbrps) {
		t.Fatalf("unexpected dbrps got %s exp %s", ti.DBRPs, dbrps)
	}
	if ti.TICKscript != tick {
		t.Fatalf("unexpected TICKscript got %s exp %s", ti.TICKscript, tick)
	}
	if !ti.Executing {
		t.Fatal("expected task to be executing")
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

	_, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), id)

	// Request data before any writes and expect null responses
	nullResponse := `{"series":null}`
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
	stopTimeC := make(chan time.Time, 1)

	db := NewInfluxDB(func(q string) *iclient.Response {
		stmt, err := influxql.ParseStatement(q)
		if err != nil {
			return &iclient.Response{Err: err.Error()}
		}
		slct, ok := stmt.(*influxql.SelectStatement)
		if !ok {
			return nil
		}
		cond, ok := slct.Condition.(*influxql.BinaryExpr)
		if !ok {
			return &iclient.Response{Err: "expected select condition to be binary expression"}
		}
		stopTimeExpr, ok := cond.RHS.(*influxql.BinaryExpr)
		if !ok {
			return &iclient.Response{Err: "expected select condition rhs to be binary expression"}
		}
		stopTL, ok := stopTimeExpr.RHS.(*influxql.StringLiteral)
		if !ok {
			return &iclient.Response{Err: "expected select condition rhs to be string literal"}
		}
		count++
		switch count {
		case 1:
			stopTime, err := time.Parse(time.RFC3339Nano, stopTL.Val)
			if err != nil {
				return &iclient.Response{Err: err.Error()}
			}
			stopTimeC <- stopTime
			return &iclient.Response{
				Results: []iclient.Result{{
					Series: []imodels.Row{{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							{
								stopTime.Add(-2 * time.Millisecond).Format(time.RFC3339Nano),
								1.0,
							},
							{
								stopTime.Add(-1 * time.Millisecond).Format(time.RFC3339Nano),
								1.0,
							},
						},
					}},
				}},
			}
		default:
			return &iclient.Response{
				Results: []iclient.Result{{
					Series: []imodels.Row{{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  [][]interface{}{},
					}},
				}},
			}
		}
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
    |query('SELECT value from mydb.myrp.cpu')
        .period(5ms)
        .every(5ms)
        .align()
    |count('value')
    |where(lambda: "count" == 2)
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

	_, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), id)

	timeout := time.NewTicker(100 * time.Millisecond)
	defer timeout.Stop()
	select {
	case <-timeout.C:
		t.Fatal("timedout waiting for query")
	case stopTime := <-stopTimeC:
		exp := fmt.Sprintf(`{"series":[{"name":"cpu","columns":["time","count"],"values":[["%s",2]]}]}`, stopTime.Local().Format(time.RFC3339Nano))
		err = s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5)
		if err != nil {
			t.Error(err)
		}
		_, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
			Status: client.Disabled,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
func TestServer_BatchTask_InfluxDBConfigUpdate(t *testing.T) {
	c := NewConfig()
	c.InfluxDB[0].Enabled = true
	count := 0
	stopTimeC := make(chan time.Time, 1)

	badCount := 0

	dbBad := NewInfluxDB(func(q string) *iclient.Response {
		badCount++
		// Return empty results
		return &iclient.Response{
			Results: []iclient.Result{},
		}
	})
	defer dbBad.Close()
	db := NewInfluxDB(func(q string) *iclient.Response {
		stmt, err := influxql.ParseStatement(q)
		if err != nil {
			return &iclient.Response{Err: err.Error()}
		}
		slct, ok := stmt.(*influxql.SelectStatement)
		if !ok {
			return nil
		}
		cond, ok := slct.Condition.(*influxql.BinaryExpr)
		if !ok {
			return &iclient.Response{Err: "expected select condition to be binary expression"}
		}
		stopTimeExpr, ok := cond.RHS.(*influxql.BinaryExpr)
		if !ok {
			return &iclient.Response{Err: "expected select condition rhs to be binary expression"}
		}
		stopTL, ok := stopTimeExpr.RHS.(*influxql.StringLiteral)
		if !ok {
			return &iclient.Response{Err: "expected select condition rhs to be string literal"}
		}
		count++
		switch count {
		case 1:
			stopTime, err := time.Parse(time.RFC3339Nano, stopTL.Val)
			if err != nil {
				return &iclient.Response{Err: err.Error()}
			}
			stopTimeC <- stopTime
			return &iclient.Response{
				Results: []iclient.Result{{
					Series: []imodels.Row{{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							{
								stopTime.Add(-2 * time.Millisecond).Format(time.RFC3339Nano),
								1.0,
							},
							{
								stopTime.Add(-1 * time.Millisecond).Format(time.RFC3339Nano),
								1.0,
							},
						},
					}},
				}},
			}
		default:
			return &iclient.Response{
				Results: []iclient.Result{{
					Series: []imodels.Row{{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  [][]interface{}{},
					}},
				}},
			}
		}
	})
	defer db.Close()

	// Set bad URL first
	c.InfluxDB[0].URLs = []string{dbBad.URL()}
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
    |query('SELECT value from mydb.myrp.cpu')
        .period(5ms)
        .every(5ms)
        .align()
    |count('value')
    |where(lambda: "count" == 2)
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

	_, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Update InfluxDB config, while task is running
	influxdbDefault := cli.ConfigElementLink("influxdb", "default")
	if err := cli.ConfigUpdate(influxdbDefault, client.ConfigUpdateAction{
		Set: map[string]interface{}{
			"urls": []string{db.URL()},
		},
	}); err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), id)
	timeout := time.NewTicker(100 * time.Millisecond)
	defer timeout.Stop()
	select {
	case <-timeout.C:
		t.Fatal("timedout waiting for query")
	case stopTime := <-stopTimeC:
		exp := fmt.Sprintf(`{"series":[{"name":"cpu","columns":["time","count"],"values":[["%s",2]]}]}`, stopTime.Local().Format(time.RFC3339Nano))
		err = s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5)
		if err != nil {
			t.Error(err)
		}
		_, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
			Status: client.Disabled,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if badCount == 0 {
		t.Error("expected bad influxdb to be queried at least once")
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

	_, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
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
		if retry > 100 {
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

	f, err := os.Open(filepath.Join(tmpDir, "alert.log"))
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
			Series: imodels.Rows{
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
	if err != nil {
		t.Error(err)
	}
	if exp, got := 1, len(recordings); exp != got {
		t.Fatalf("unexpected recordings list:\ngot %v\nexp %v\nrecordings %v", got, exp, recordings)
	}

	err = cli.DeleteRecording(recordings[0].Link)
	if err != nil {
		t.Error(err)
	}

	recordings, err = cli.ListRecordings(nil)
	if err != nil {
		t.Error(err)
	}
	if exp, got := 0, len(recordings); exp != got {
		t.Errorf("unexpected recordings list after delete:\ngot %v\nexp %v\nrecordings %v", got, exp, recordings)
	}

	replays, err := cli.ListReplays(nil)
	if err != nil {
		t.Error(err)
	}
	if exp, got := 1, len(replays); exp != got {
		t.Fatalf("unexpected replays list:\ngot %v\nexp %v\nreplays %v", got, exp, replays)
	}

	err = cli.DeleteReplay(replays[0].Link)
	if err != nil {
		t.Error(err)
	}

	replays, err = cli.ListReplays(nil)
	if err != nil {
		t.Error(err)
	}
	if exp, got := 0, len(replays); exp != got {
		t.Errorf("unexpected replays list after delete:\ngot %v\nexp %v\nreplays %v", got, exp, replays)
	}
}

func TestServer_RecordReplayStreamWithPost(t *testing.T) {
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
        .post('http://localhost:8080')
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
		if retry > 100 {
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

	f, err := os.Open(filepath.Join(tmpDir, "alert.log"))
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
			Series: imodels.Rows{
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
	if err != nil {
		t.Error(err)
	}
	if exp, got := 1, len(recordings); exp != got {
		t.Fatalf("unexpected recordings list:\ngot %v\nexp %v\nrecordings %v", got, exp, recordings)
	}

	err = cli.DeleteRecording(recordings[0].Link)
	if err != nil {
		t.Error(err)
	}

	recordings, err = cli.ListRecordings(nil)
	if err != nil {
		t.Error(err)
	}
	if exp, got := 0, len(recordings); exp != got {
		t.Errorf("unexpected recordings list after delete:\ngot %v\nexp %v\nrecordings %v", got, exp, recordings)
	}

	replays, err := cli.ListReplays(nil)
	if err != nil {
		t.Error(err)
	}
	if exp, got := 1, len(replays); exp != got {
		t.Fatalf("unexpected replays list:\ngot %v\nexp %v\nreplays %v", got, exp, replays)
	}

	err = cli.DeleteReplay(replays[0].Link)
	if err != nil {
		t.Error(err)
	}

	replays, err = cli.ListReplays(nil)
	if err != nil {
		t.Error(err)
	}
	if exp, got := 0, len(replays); exp != got {
		t.Errorf("unexpected replays list after delete:\ngot %v\nexp %v\nreplays %v", got, exp, replays)
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
					Series: []imodels.Row{{
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
			t.Fatal("failed to perform replay")
		}
	}

	f, err := os.Open(filepath.Join(tmpDir, "alert.log"))
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
				Series: imodels.Rows{
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
				Series: imodels.Rows{
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
	dec := json.NewDecoder(f)
	got := make([]response, 0)
	for dec.More() {
		g := response{}
		dec.Decode(&g)
		got = append(got, g)
	}
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got, exp)
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got[0].Data.Series[0], exp[0].Data.Series[0])
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got[1].Data.Series[0], exp[1].Data.Series[0])
	}

	recordings, err := cli.ListRecordings(nil)
	if err != nil {
		t.Error(err)
	}
	if exp, got := 1, len(recordings); exp != got {
		t.Fatalf("unexpected recordings list:\ngot %v\nexp %v", got, exp)
	}

	err = cli.DeleteRecording(recordings[0].Link)
	if err != nil {
		t.Error(err)
	}

	recordings, err = cli.ListRecordings(nil)
	if err != nil {
		t.Error(err)
	}
	if exp, got := 0, len(recordings); exp != got {
		t.Errorf("unexpected recordings list:\ngot %v\nexp %v", got, exp)
	}

	replays, err := cli.ListReplays(nil)
	if err != nil {
		t.Error(err)
	}
	if exp, got := 1, len(replays); exp != got {
		t.Fatalf("unexpected replays list:\ngot %v\nexp %v", got, exp)
	}

	err = cli.DeleteReplay(replays[0].Link)
	if err != nil {
		t.Error(err)
	}

	replays, err = cli.ListReplays(nil)
	if err != nil {
		t.Error(err)
	}
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
					Series: []imodels.Row{{
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

	f, err := os.Open(filepath.Join(tmpDir, "alert.log"))
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
				Series: imodels.Rows{
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
				Series: imodels.Rows{
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
	dec := json.NewDecoder(f)
	got := make([]response, 0)
	for dec.More() {
		g := response{}
		dec.Decode(&g)
		got = append(got, g)
	}
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got, exp)
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got[0].Data.Series[0], exp[0].Data.Series[0])
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got[1].Data.Series[0], exp[1].Data.Series[0])
	}

	recordings, err := cli.ListRecordings(nil)
	if err != nil {
		t.Error(err)
	}
	if exp, got := 0, len(recordings); exp != got {
		t.Fatalf("unexpected recordings list:\ngot %v\nexp %v", got, exp)
	}

	replays, err := cli.ListReplays(nil)
	if err != nil {
		t.Error(err)
	}
	if exp, got := 1, len(replays); exp != got {
		t.Fatalf("unexpected replays list:\ngot %v\nexp %v", got, exp)
	}

	err = cli.DeleteReplay(replays[0].Link)
	if err != nil {
		t.Error(err)
	}

	replays, err = cli.ListReplays(nil)
	if err != nil {
		t.Error(err)
	}
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
					Series: []imodels.Row{
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

	f, err := os.Open(filepath.Join(tmpDir, "alert.log"))
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
				Series: imodels.Rows{
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
				Series: imodels.Rows{
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
	dec := json.NewDecoder(f)
	got := make([]response, 0)
	for dec.More() {
		g := response{}
		dec.Decode(&g)
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
	if err != nil {
		t.Error(err)
	}
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
	dec = json.NewDecoder(resp.Body)
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
	if err != nil {
		t.Error(err)
	}
	if exp, got := 0, len(recordings); exp != got {
		t.Errorf("unexpected recordings list:\ngot %v\nexp %v", got, exp)
	}

	replays, err := cli.ListReplays(nil)
	if err != nil {
		t.Error(err)
	}
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
	if err != nil {
		t.Error(err)
	}
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
					Series: []imodels.Row{
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

	f, err := os.Open(filepath.Join(tmpDir, "alert.log"))
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
				Series: imodels.Rows{
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
				Series: imodels.Rows{
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
	dec := json.NewDecoder(f)
	got := make([]response, 0)
	for dec.More() {
		g := response{}
		dec.Decode(&g)
		got = append(got, g)
	}
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got, exp)
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got[0].Data.Series[0], exp[0].Data.Series[0])
		t.Errorf("unexpected alert log:\ngot %v\nexp %v", got[1].Data.Series[0], exp[1].Data.Series[0])
	}

	recordings, err := cli.ListRecordings(nil)
	if err != nil {
		t.Error(err)
	}
	if exp, got := 0, len(recordings); exp != got {
		t.Fatalf("unexpected recordings list:\ngot %v\nexp %v", got, exp)
	}

	replays, err := cli.ListReplays(nil)
	if err != nil {
		t.Error(err)
	}
	if exp, got := 1, len(replays); exp != got {
		t.Fatalf("unexpected replays list:\ngot %v\nexp %v", got, exp)
	}

	err = cli.DeleteReplay(replays[0].Link)
	if err != nil {
		t.Error(err)
	}

	replays, err = cli.ListReplays(nil)
	if err != nil {
		t.Error(err)
	}
	if exp, got := 0, len(replays); exp != got {
		t.Errorf("unexpected replays list:\ngot %v\nexp %v", got, exp)
	}
}

// Test for recording and replaying a stream query where data has missing fields and tags.
func TestServer_RecordReplayQuery_Missing(t *testing.T) {
	c := NewConfig()
	c.InfluxDB[0].Enabled = true
	db := NewInfluxDB(func(q string) *iclient.Response {
		if len(q) > 6 && q[:6] == "SELECT" {
			r := &iclient.Response{
				Results: []iclient.Result{{
					Series: []imodels.Row{
						{
							Name:    "m",
							Tags:    map[string]string{"t1": "", "t2": ""},
							Columns: []string{"time", "a", "b"},
							Values: [][]interface{}{
								{
									time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC).Format(time.RFC3339Nano),
									1.0,
									nil,
								},
								{
									time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC).Format(time.RFC3339Nano),
									nil,
									2.0,
								},
								{
									time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC).Format(time.RFC3339Nano),
									nil,
									10.0,
								},
								{
									time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC).Format(time.RFC3339Nano),
									11.0,
									nil,
								},
							},
						},
						{
							Name:    "m",
							Tags:    map[string]string{"t1": "", "t2": "4"},
							Columns: []string{"time", "a", "b"},
							Values: [][]interface{}{
								{
									time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC).Format(time.RFC3339Nano),
									4.0,
									4.0,
								},
							},
						},
						{
							Name:    "m",
							Tags:    map[string]string{"t1": "", "t2": "7"},
							Columns: []string{"time", "a", "b"},
							Values: [][]interface{}{
								{
									time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC).Format(time.RFC3339Nano),
									nil,
									7.0,
								},
							},
						},
						{
							Name:    "m",
							Tags:    map[string]string{"t1": "3", "t2": ""},
							Columns: []string{"time", "a", "b"},
							Values: [][]interface{}{
								{
									time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC).Format(time.RFC3339Nano),
									3.0,
									3.0,
								},
							},
						},
						{
							Name:    "m",
							Tags:    map[string]string{"t1": "5", "t2": ""},
							Columns: []string{"time", "a", "b"},
							Values: [][]interface{}{
								{
									time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC).Format(time.RFC3339Nano),
									5.0,
									5.0,
								},
							},
						},
						{
							Name:    "m",
							Tags:    map[string]string{"t1": "6", "t2": ""},
							Columns: []string{"time", "a", "b"},
							Values: [][]interface{}{
								{
									time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC).Format(time.RFC3339Nano),
									nil,
									6.0,
								},
							},
						},
						{
							Name:    "m",
							Tags:    map[string]string{"t1": "8", "t2": ""},
							Columns: []string{"time", "a", "b"},
							Values: [][]interface{}{
								{
									time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC).Format(time.RFC3339Nano),
									nil,
									8.0,
								},
							},
						},
						{
							Name:    "m",
							Tags:    map[string]string{"t1": "9", "t2": ""},
							Columns: []string{"time", "a", "b"},
							Values: [][]interface{}{
								{
									time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC).Format(time.RFC3339Nano),
									nil,
									9.0,
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

	id := "testStreamQueryRecordReplay"
	ttype := client.StreamTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}

	// setup temp dir for alert.log
	tmpDir, err := ioutil.TempDir("", "testStreamTaskRecordingReplay")
	if err != nil {
		t.Fatal(err)
	}
	//defer os.RemoveAll(tmpDir)

	tick := `stream
	|from()
		.measurement('m')
	|log()
	|alert()
		.id('test-stream-query')
		.crit(lambda: TRUE)
		.details('')
		.log('` + tmpDir + `/alert.log')
`

	if _, err := cli.CreateTask(client.CreateTaskOptions{
		ID:         id,
		Type:       ttype,
		DBRPs:      dbrps,
		TICKscript: tick,
		Status:     client.Disabled,
	}); err != nil {
		t.Fatal(err)
	}

	recording, err := cli.RecordQuery(client.RecordQueryOptions{
		ID:    "recordingid",
		Query: "SELECT * FROM mydb.myrp.m",
		Type:  client.StreamTask,
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

	// Validate we got the data in the alert.log

	f, err := os.Open(filepath.Join(tmpDir, "alert.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	exp := []alert.Data{
		{
			ID:            "test-stream-query",
			Message:       "test-stream-query is CRITICAL",
			Time:          time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.OK,
			Duration:      0 * time.Second,
			Recoverable:   true,
			Data: models.Result{
				Series: models.Rows{
					{
						Name:    "m",
						Columns: []string{"time", "a"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
								1.0,
							},
						},
					},
				},
			},
		},
		{
			ID:            "test-stream-query",
			Message:       "test-stream-query is CRITICAL",
			Time:          time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      1 * time.Second,
			Recoverable:   true,
			Data: models.Result{
				Series: models.Rows{
					{
						Name:    "m",
						Columns: []string{"time", "b"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
								2.0,
							},
						},
					},
				},
			},
		},
		{
			ID:            "test-stream-query",
			Message:       "test-stream-query is CRITICAL",
			Time:          time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      2 * time.Second,
			Recoverable:   true,
			Data: models.Result{
				Series: models.Rows{
					{
						Name:    "m",
						Tags:    map[string]string{"t1": "3"},
						Columns: []string{"time", "a", "b"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
								3.0,
								3.0,
							},
						},
					},
				},
			},
		},
		{
			ID:            "test-stream-query",
			Message:       "test-stream-query is CRITICAL",
			Time:          time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      3 * time.Second,
			Recoverable:   true,
			Data: models.Result{
				Series: models.Rows{
					{
						Name:    "m",
						Tags:    map[string]string{"t2": "4"},
						Columns: []string{"time", "a", "b"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
								4.0,
								4.0,
							},
						},
					},
				},
			},
		},
		{
			ID:            "test-stream-query",
			Message:       "test-stream-query is CRITICAL",
			Time:          time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      4 * time.Second,
			Recoverable:   true,
			Data: models.Result{
				Series: models.Rows{
					{
						Name:    "m",
						Tags:    map[string]string{"t1": "5"},
						Columns: []string{"time", "a", "b"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
								5.0,
								5.0,
							},
						},
					},
				},
			},
		},
		{
			ID:            "test-stream-query",
			Message:       "test-stream-query is CRITICAL",
			Time:          time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      5 * time.Second,
			Recoverable:   true,
			Data: models.Result{
				Series: models.Rows{
					{
						Name:    "m",
						Tags:    map[string]string{"t1": "6"},
						Columns: []string{"time", "b"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
								6.0,
							},
						},
					},
				},
			},
		},
		{
			ID:            "test-stream-query",
			Message:       "test-stream-query is CRITICAL",
			Time:          time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      6 * time.Second,
			Recoverable:   true,
			Data: models.Result{
				Series: models.Rows{
					{
						Name:    "m",
						Tags:    map[string]string{"t2": "7"},
						Columns: []string{"time", "b"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
								7.0,
							},
						},
					},
				},
			},
		},
		{
			ID:            "test-stream-query",
			Message:       "test-stream-query is CRITICAL",
			Time:          time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      7 * time.Second,
			Recoverable:   true,
			Data: models.Result{
				Series: models.Rows{
					{
						Name:    "m",
						Tags:    map[string]string{"t1": "8"},
						Columns: []string{"time", "b"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
								8.0,
							},
						},
					},
				},
			},
		},
		{
			ID:            "test-stream-query",
			Message:       "test-stream-query is CRITICAL",
			Time:          time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      8 * time.Second,
			Recoverable:   true,
			Data: models.Result{
				Series: models.Rows{
					{
						Name:    "m",
						Tags:    map[string]string{"t1": "9"},
						Columns: []string{"time", "b"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
								9.0,
							},
						},
					},
				},
			},
		},
		{
			ID:            "test-stream-query",
			Message:       "test-stream-query is CRITICAL",
			Time:          time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      9 * time.Second,
			Recoverable:   true,
			Data: models.Result{
				Series: models.Rows{
					{
						Name:    "m",
						Columns: []string{"time", "b"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
								10.0,
							},
						},
					},
				},
			},
		},
		{
			ID:            "test-stream-query",
			Message:       "test-stream-query is CRITICAL",
			Time:          time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      10 * time.Second,
			Recoverable:   true,
			Data: models.Result{
				Series: models.Rows{
					{
						Name:    "m",
						Columns: []string{"time", "a"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
								11.0,
							},
						},
					},
				},
			},
		},
	}
	dec := json.NewDecoder(f)
	var got []alert.Data
	for dec.More() {
		g := alert.Data{}
		dec.Decode(&g)
		got = append(got, g)
	}
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("unexpected alert log:\ngot %+v\nexp %+v", got, exp)
	}
}

// If this test fails due to missing python dependencies, run 'INSTALL_PREFIX=/usr/local ./install-deps.sh' from the root directory of the
// kapacitor project.
func TestServer_UDFStreamAgents(t *testing.T) {
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
					filepath.Join(tdir, "movavg"+ExecutableSuffix),
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
				Prog:    PythonExecutable,
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

func testStreamAgent(t *testing.T, c *server.Config) {
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

	_, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("%s/tasks/%s/moving_avg", s.URL(), id)

	// Request data before any writes and expect null responses
	nullResponse := `{"series":null}`
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
	if runtime.GOOS == "windows" {
		t.Skip("Skipping on windows as unix sockets are not available")
	}
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
					filepath.Join(tdir, "mirror"+ExecutableSuffix),
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
					PythonExecutable,
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

func testStreamAgentSocket(t *testing.T, c *server.Config) {
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

	_, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), id)

	// Request data before any writes and expect null responses
	nullResponse := `{"series":null}`
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
					filepath.Join(tdir, "outliers"+ExecutableSuffix),
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
				Prog:    PythonExecutable,
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

func testBatchAgent(t *testing.T, c *server.Config) {
	count := 0
	stopTimeC := make(chan time.Time, 2)
	db := NewInfluxDB(func(q string) *iclient.Response {
		stmt, err := influxql.ParseStatement(q)
		if err != nil {
			return &iclient.Response{Err: err.Error()}
		}
		slct, ok := stmt.(*influxql.SelectStatement)
		if !ok {
			return nil
		}
		cond, ok := slct.Condition.(*influxql.BinaryExpr)
		if !ok {
			return &iclient.Response{Err: "expected select condition to be binary expression"}
		}
		stopTimeExpr, ok := cond.RHS.(*influxql.BinaryExpr)
		if !ok {
			return &iclient.Response{Err: "expected select condition rhs to be binary expression"}
		}
		stopTL, ok := stopTimeExpr.RHS.(*influxql.StringLiteral)
		if !ok {
			return &iclient.Response{Err: "expected select condition rhs to be string literal"}
		}
		count++
		switch count {
		case 1, 2:
			stopTime, err := time.Parse(time.RFC3339Nano, stopTL.Val)
			if err != nil {
				return &iclient.Response{Err: err.Error()}
			}
			stopTimeC <- stopTime
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
					stopTime.Add(time.Duration(i-len(data)) * time.Millisecond).Format(time.RFC3339Nano),
					value,
				}
			}

			return &iclient.Response{
				Results: []iclient.Result{{
					Series: []imodels.Row{{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Tags: map[string]string{
							"count": strconv.FormatInt(int64(count%2), 10),
						},
						Values: values,
					}},
				}},
			}
		default:
			return nil
		}
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

	_, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	stopTimes := make([]time.Time, 2)
	for i := range stopTimes {
		timeout := time.NewTicker(100 * time.Millisecond)
		defer timeout.Stop()
		select {
		case <-timeout.C:
			t.Fatal("timedout waiting for query")
		case stopTime := <-stopTimeC:
			stopTimes[i] = stopTime
		}
	}
	endpoint := fmt.Sprintf("%s/tasks/%s/count", s.URL(), id)
	exp := fmt.Sprintf(
		`{"series":[{"name":"cpu","tags":{"count":"1"},"columns":["time","count"],"values":[["%s",5]]},{"name":"cpu","tags":{"count":"0"},"columns":["time","count"],"values":[["%s",5]]}]}`,
		stopTimes[0].Format(time.RFC3339Nano),
		stopTimes[1].Format(time.RFC3339Nano),
	)
	err = s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*50)
	if err != nil {
		t.Error(err)
	}
	_, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
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

func TestServer_UpdateConfig(t *testing.T) {
	type updateAction struct {
		element      string
		updateAction client.ConfigUpdateAction
		expSection   client.ConfigSection
		expElement   client.ConfigElement
	}
	db := NewInfluxDB(func(q string) *iclient.Response {
		return &iclient.Response{}
	})
	testCases := []struct {
		section           string
		element           string
		setDefaults       func(*server.Config)
		expDefaultSection client.ConfigSection
		expDefaultElement client.ConfigElement
		updates           []updateAction
	}{
		{
			section: "influxdb",
			element: "default",
			setDefaults: func(c *server.Config) {
				c.InfluxDB[0].Enabled = true
				c.InfluxDB[0].Username = "bob"
				c.InfluxDB[0].Password = "secret"
				c.InfluxDB[0].URLs = []string{db.URL()}
				// Set really long timeout since we shouldn't hit it
				c.InfluxDB[0].StartUpTimeout = toml.Duration(time.Hour)
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
					Options: map[string]interface{}{
						"default":                     false,
						"disable-subscriptions":       false,
						"enabled":                     true,
						"excluded-subscriptions":      map[string]interface{}{"_kapacitor": []interface{}{"autogen"}},
						"http-port":                   float64(0),
						"insecure-skip-verify":        false,
						"kapacitor-hostname":          "",
						"name":                        "default",
						"password":                    true,
						"ssl-ca":                      "",
						"ssl-cert":                    "",
						"ssl-key":                     "",
						"startup-timeout":             "1h0m0s",
						"subscription-protocol":       "http",
						"subscription-mode":           "cluster",
						"subscriptions":               nil,
						"subscriptions-sync-interval": "1m0s",
						"timeout":                     "0s",
						"udp-bind":                    "",
						"udp-buffer":                  float64(1e3),
						"udp-read-buffer":             float64(0),
						"urls":                        []interface{}{db.URL()},
						"username":                    "bob",
					},
					Redacted: []string{
						"password",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
				Options: map[string]interface{}{
					"default":                     false,
					"disable-subscriptions":       false,
					"enabled":                     true,
					"excluded-subscriptions":      map[string]interface{}{"_kapacitor": []interface{}{"autogen"}},
					"http-port":                   float64(0),
					"insecure-skip-verify":        false,
					"kapacitor-hostname":          "",
					"name":                        "default",
					"password":                    true,
					"ssl-ca":                      "",
					"ssl-cert":                    "",
					"ssl-key":                     "",
					"startup-timeout":             "1h0m0s",
					"subscription-protocol":       "http",
					"subscription-mode":           "cluster",
					"subscriptions":               nil,
					"subscriptions-sync-interval": "1m0s",
					"timeout":                     "0s",
					"udp-bind":                    "",
					"udp-buffer":                  float64(1e3),
					"udp-read-buffer":             float64(0),
					"urls":                        []interface{}{db.URL()},
					"username":                    "bob",
				},
				Redacted: []string{
					"password",
				},
			},
			updates: []updateAction{
				{
					// Set Invalid URL to make sure we can fix it without waiting for connection timeouts
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"urls": []string{"http://192.0.2.0:8086"},
						},
					},
					element: "default",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
							Options: map[string]interface{}{
								"default":                     false,
								"disable-subscriptions":       false,
								"enabled":                     true,
								"excluded-subscriptions":      map[string]interface{}{"_kapacitor": []interface{}{"autogen"}},
								"http-port":                   float64(0),
								"insecure-skip-verify":        false,
								"kapacitor-hostname":          "",
								"name":                        "default",
								"password":                    true,
								"ssl-ca":                      "",
								"ssl-cert":                    "",
								"ssl-key":                     "",
								"startup-timeout":             "1h0m0s",
								"subscription-protocol":       "http",
								"subscription-mode":           "cluster",
								"subscriptions":               nil,
								"subscriptions-sync-interval": "1m0s",
								"timeout":                     "0s",
								"udp-bind":                    "",
								"udp-buffer":                  float64(1e3),
								"udp-read-buffer":             float64(0),
								"urls":                        []interface{}{"http://192.0.2.0:8086"},
								"username":                    "bob",
							},
							Redacted: []string{
								"password",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
						Options: map[string]interface{}{
							"default":                     false,
							"disable-subscriptions":       false,
							"enabled":                     true,
							"excluded-subscriptions":      map[string]interface{}{"_kapacitor": []interface{}{"autogen"}},
							"http-port":                   float64(0),
							"insecure-skip-verify":        false,
							"kapacitor-hostname":          "",
							"name":                        "default",
							"password":                    true,
							"ssl-ca":                      "",
							"ssl-cert":                    "",
							"ssl-key":                     "",
							"startup-timeout":             "1h0m0s",
							"subscription-protocol":       "http",
							"subscription-mode":           "cluster",
							"subscriptions":               nil,
							"subscriptions-sync-interval": "1m0s",
							"timeout":                     "0s",
							"udp-bind":                    "",
							"udp-buffer":                  float64(1e3),
							"udp-read-buffer":             float64(0),
							"urls":                        []interface{}{"http://192.0.2.0:8086"},
							"username":                    "bob",
						},
						Redacted: []string{
							"password",
						},
					},
				},
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"default":               true,
							"subscription-protocol": "https",
							"subscriptions":         map[string][]string{"_internal": []string{"monitor"}},
						},
					},
					element: "default",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
							Options: map[string]interface{}{
								"default":                     true,
								"disable-subscriptions":       false,
								"enabled":                     true,
								"excluded-subscriptions":      map[string]interface{}{"_kapacitor": []interface{}{"autogen"}},
								"http-port":                   float64(0),
								"insecure-skip-verify":        false,
								"kapacitor-hostname":          "",
								"name":                        "default",
								"password":                    true,
								"ssl-ca":                      "",
								"ssl-cert":                    "",
								"ssl-key":                     "",
								"startup-timeout":             "1h0m0s",
								"subscription-protocol":       "https",
								"subscription-mode":           "cluster",
								"subscriptions":               map[string]interface{}{"_internal": []interface{}{"monitor"}},
								"subscriptions-sync-interval": "1m0s",
								"timeout":                     "0s",
								"udp-bind":                    "",
								"udp-buffer":                  float64(1e3),
								"udp-read-buffer":             float64(0),
								"urls":                        []interface{}{"http://192.0.2.0:8086"},
								"username":                    "bob",
							},
							Redacted: []string{
								"password",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
						Options: map[string]interface{}{
							"default":                     true,
							"disable-subscriptions":       false,
							"enabled":                     true,
							"excluded-subscriptions":      map[string]interface{}{"_kapacitor": []interface{}{"autogen"}},
							"http-port":                   float64(0),
							"insecure-skip-verify":        false,
							"kapacitor-hostname":          "",
							"name":                        "default",
							"password":                    true,
							"ssl-ca":                      "",
							"ssl-cert":                    "",
							"ssl-key":                     "",
							"startup-timeout":             "1h0m0s",
							"subscription-protocol":       "https",
							"subscription-mode":           "cluster",
							"subscriptions":               map[string]interface{}{"_internal": []interface{}{"monitor"}},
							"subscriptions-sync-interval": "1m0s",
							"timeout":                     "0s",
							"udp-bind":                    "",
							"udp-buffer":                  float64(1e3),
							"udp-read-buffer":             float64(0),
							"urls":                        []interface{}{"http://192.0.2.0:8086"},
							"username":                    "bob",
						},
						Redacted: []string{
							"password",
						},
					},
				},
				{
					updateAction: client.ConfigUpdateAction{
						Delete: []string{"urls"},
					},
					element: "default",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
							Options: map[string]interface{}{
								"default":                     true,
								"disable-subscriptions":       false,
								"enabled":                     true,
								"excluded-subscriptions":      map[string]interface{}{"_kapacitor": []interface{}{"autogen"}},
								"http-port":                   float64(0),
								"insecure-skip-verify":        false,
								"kapacitor-hostname":          "",
								"name":                        "default",
								"password":                    true,
								"ssl-ca":                      "",
								"ssl-cert":                    "",
								"ssl-key":                     "",
								"startup-timeout":             "1h0m0s",
								"subscription-protocol":       "https",
								"subscription-mode":           "cluster",
								"subscriptions":               map[string]interface{}{"_internal": []interface{}{"monitor"}},
								"subscriptions-sync-interval": "1m0s",
								"timeout":                     "0s",
								"udp-bind":                    "",
								"udp-buffer":                  float64(1e3),
								"udp-read-buffer":             float64(0),
								"urls":                        []interface{}{db.URL()},
								"username":                    "bob",
							},
							Redacted: []string{
								"password",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
						Options: map[string]interface{}{
							"default":                     true,
							"disable-subscriptions":       false,
							"enabled":                     true,
							"excluded-subscriptions":      map[string]interface{}{"_kapacitor": []interface{}{"autogen"}},
							"http-port":                   float64(0),
							"insecure-skip-verify":        false,
							"kapacitor-hostname":          "",
							"name":                        "default",
							"password":                    true,
							"ssl-ca":                      "",
							"ssl-cert":                    "",
							"ssl-key":                     "",
							"startup-timeout":             "1h0m0s",
							"subscription-protocol":       "https",
							"subscription-mode":           "cluster",
							"subscriptions":               map[string]interface{}{"_internal": []interface{}{"monitor"}},
							"subscriptions-sync-interval": "1m0s",
							"timeout":                     "0s",
							"udp-bind":                    "",
							"udp-buffer":                  float64(1e3),
							"udp-read-buffer":             float64(0),
							"urls":                        []interface{}{db.URL()},
							"username":                    "bob",
						},
						Redacted: []string{
							"password",
						},
					},
				},
				{
					updateAction: client.ConfigUpdateAction{
						Add: map[string]interface{}{
							"name": "new",
							"urls": []string{db.URL()},
						},
					},
					element: "new",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb"},
						Elements: []client.ConfigElement{
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
								Options: map[string]interface{}{
									"default":                     true,
									"disable-subscriptions":       false,
									"enabled":                     true,
									"excluded-subscriptions":      map[string]interface{}{"_kapacitor": []interface{}{"autogen"}},
									"http-port":                   float64(0),
									"insecure-skip-verify":        false,
									"kapacitor-hostname":          "",
									"name":                        "default",
									"password":                    true,
									"ssl-ca":                      "",
									"ssl-cert":                    "",
									"ssl-key":                     "",
									"startup-timeout":             "1h0m0s",
									"subscription-protocol":       "https",
									"subscription-mode":           "cluster",
									"subscriptions":               map[string]interface{}{"_internal": []interface{}{"monitor"}},
									"subscriptions-sync-interval": "1m0s",
									"timeout":                     "0s",
									"udp-bind":                    "",
									"udp-buffer":                  float64(1e3),
									"udp-read-buffer":             float64(0),
									"urls":                        []interface{}{db.URL()},
									"username":                    "bob",
								},
								Redacted: []string{
									"password",
								},
							},
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/new"},
								Options: map[string]interface{}{
									"default":                     false,
									"disable-subscriptions":       false,
									"enabled":                     false,
									"excluded-subscriptions":      map[string]interface{}{"_kapacitor": []interface{}{"autogen"}},
									"http-port":                   float64(0),
									"insecure-skip-verify":        false,
									"kapacitor-hostname":          "",
									"name":                        "new",
									"password":                    false,
									"ssl-ca":                      "",
									"ssl-cert":                    "",
									"ssl-key":                     "",
									"startup-timeout":             "5m0s",
									"subscription-protocol":       "http",
									"subscription-mode":           "cluster",
									"subscriptions":               nil,
									"subscriptions-sync-interval": "1m0s",
									"timeout":                     "0s",
									"udp-bind":                    "",
									"udp-buffer":                  float64(1e3),
									"udp-read-buffer":             float64(0),
									"urls":                        []interface{}{db.URL()},
									"username":                    "",
								},
								Redacted: []string{
									"password",
								},
							},
						},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/new"},
						Options: map[string]interface{}{
							"default":                     false,
							"disable-subscriptions":       false,
							"enabled":                     false,
							"excluded-subscriptions":      map[string]interface{}{"_kapacitor": []interface{}{"autogen"}},
							"http-port":                   float64(0),
							"insecure-skip-verify":        false,
							"kapacitor-hostname":          "",
							"name":                        "new",
							"password":                    false,
							"ssl-ca":                      "",
							"ssl-cert":                    "",
							"ssl-key":                     "",
							"startup-timeout":             "5m0s",
							"subscription-protocol":       "http",
							"subscriptions":               nil,
							"subscription-mode":           "cluster",
							"subscriptions-sync-interval": "1m0s",
							"timeout":                     "0s",
							"udp-bind":                    "",
							"udp-buffer":                  float64(1e3),
							"udp-read-buffer":             float64(0),
							"urls":                        []interface{}{db.URL()},
							"username":                    "",
						},
						Redacted: []string{
							"password",
						},
					},
				},
			},
		},
		{
			section: "alerta",
			setDefaults: func(c *server.Config) {
				c.Alerta.URL = "http://alerta.example.com"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/alerta"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/alerta/"},
					Options: map[string]interface{}{
						"enabled":              false,
						"environment":          "",
						"origin":               "",
						"token":                false,
						"token-prefix":         "",
						"url":                  "http://alerta.example.com",
						"insecure-skip-verify": false,
						"timeout":              "0s",
					},
					Redacted: []string{
						"token",
					}},
				},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/alerta/"},
				Options: map[string]interface{}{
					"enabled":              false,
					"environment":          "",
					"origin":               "",
					"token":                false,
					"token-prefix":         "",
					"url":                  "http://alerta.example.com",
					"insecure-skip-verify": false,
					"timeout":              "0s",
				},
				Redacted: []string{
					"token",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"token":   "token",
							"origin":  "kapacitor",
							"timeout": "3h",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/alerta"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/alerta/"},
							Options: map[string]interface{}{
								"enabled":              false,
								"environment":          "",
								"origin":               "kapacitor",
								"token":                true,
								"token-prefix":         "",
								"url":                  "http://alerta.example.com",
								"insecure-skip-verify": false,
								"timeout":              "3h0m0s",
							},
							Redacted: []string{
								"token",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/alerta/"},
						Options: map[string]interface{}{
							"enabled":              false,
							"environment":          "",
							"origin":               "kapacitor",
							"token":                true,
							"token-prefix":         "",
							"url":                  "http://alerta.example.com",
							"insecure-skip-verify": false,
							"timeout":              "3h0m0s",
						},
						Redacted: []string{
							"token",
						},
					},
				},
			},
		},
		{
			section: "httppost",
			element: "test",
			setDefaults: func(c *server.Config) {
				apc := httppost.Config{
					Endpoint: "test",
					URL:      "http://httppost.example.com",
					Headers: map[string]string{
						"testing": "works",
					},
				}
				c.HTTPPost = httppost.Configs{apc}
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/httppost"},
				Elements: []client.ConfigElement{
					{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/httppost/test"},
						Options: map[string]interface{}{
							"endpoint": "test",
							"url":      "http://httppost.example.com",
							"headers": map[string]interface{}{
								"testing": "works",
							},
							"basic-auth":          false,
							"alert-template":      "",
							"alert-template-file": "",
							"row-template":        "",
							"row-template-file":   "",
						},
						Redacted: []string{
							"basic-auth",
						}},
				},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/httppost/test"},
				Options: map[string]interface{}{
					"endpoint": "test",
					"url":      "http://httppost.example.com",
					"headers": map[string]interface{}{
						"testing": "works",
					},
					"basic-auth":          false,
					"alert-template":      "",
					"alert-template-file": "",
					"row-template":        "",
					"row-template-file":   "",
				},
				Redacted: []string{
					"basic-auth",
				},
			},
			updates: []updateAction{
				{
					element: "test",
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"headers": map[string]string{
								"testing": "more",
							},
							"basic-auth": httppost.BasicAuth{
								Username: "usr",
								Password: "pass",
							},
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/httppost"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/httppost/test"},
							Options: map[string]interface{}{
								"endpoint": "test",
								"url":      "http://httppost.example.com",
								"headers": map[string]interface{}{
									"testing": "more",
								},
								"basic-auth":          true,
								"alert-template":      "",
								"alert-template-file": "",
								"row-template":        "",
								"row-template-file":   "",
							},
							Redacted: []string{
								"basic-auth",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/httppost/test"},
						Options: map[string]interface{}{
							"endpoint": "test",
							"url":      "http://httppost.example.com",
							"headers": map[string]interface{}{
								"testing": "more",
							},
							"basic-auth":          true,
							"alert-template":      "",
							"alert-template-file": "",
							"row-template":        "",
							"row-template-file":   "",
						},
						Redacted: []string{
							"basic-auth",
						},
					},
				},
			},
		},
		{
			section: "pushover",
			setDefaults: func(c *server.Config) {
				c.Pushover.URL = "http://pushover.example.com"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pushover"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pushover/"},
					Options: map[string]interface{}{
						"enabled":  false,
						"token":    false,
						"user-key": false,
						"url":      "http://pushover.example.com",
					},
					Redacted: []string{
						"token",
						"user-key",
					}},
				},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pushover/"},
				Options: map[string]interface{}{
					"enabled":  false,
					"token":    false,
					"user-key": false,
					"url":      "http://pushover.example.com",
				},
				Redacted: []string{
					"token",
					"user-key",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"token":    "token",
							"user-key": "kapacitor",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pushover"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pushover/"},
							Options: map[string]interface{}{
								"enabled":  false,
								"user-key": true,
								"token":    true,
								"url":      "http://pushover.example.com",
							},
							Redacted: []string{
								"token",
								"user-key",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pushover/"},
						Options: map[string]interface{}{
							"enabled":  false,
							"user-key": true,
							"token":    true,
							"url":      "http://pushover.example.com",
						},
						Redacted: []string{
							"token",
							"user-key",
						},
					},
				},
			},
		},
		{
			section: "kubernetes",
			setDefaults: func(c *server.Config) {
				c.Kubernetes = k8s.Configs{k8s.NewConfig()}
				c.Kubernetes[0].APIServers = []string{"http://localhost:80001"}
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/kubernetes"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/kubernetes/"},
					Options: map[string]interface{}{
						"id":          "",
						"api-servers": []interface{}{"http://localhost:80001"},
						"ca-path":     "",
						"enabled":     false,
						"in-cluster":  false,
						"namespace":   "",
						"token":       false,
						"resource":    "",
					},
					Redacted: []string{
						"token",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/kubernetes/"},
				Options: map[string]interface{}{
					"id":          "",
					"api-servers": []interface{}{"http://localhost:80001"},
					"ca-path":     "",
					"enabled":     false,
					"in-cluster":  false,
					"namespace":   "",
					"token":       false,
					"resource":    "",
				},
				Redacted: []string{
					"token",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"token": "secret",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/kubernetes"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/kubernetes/"},
							Options: map[string]interface{}{
								"id":          "",
								"api-servers": []interface{}{"http://localhost:80001"},
								"ca-path":     "",
								"enabled":     false,
								"in-cluster":  false,
								"namespace":   "",
								"token":       true,
								"resource":    "",
							},
							Redacted: []string{
								"token",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/kubernetes/"},
						Options: map[string]interface{}{
							"id":          "",
							"api-servers": []interface{}{"http://localhost:80001"},
							"ca-path":     "",
							"enabled":     false,
							"in-cluster":  false,
							"namespace":   "",
							"token":       true,
							"resource":    "",
						},
						Redacted: []string{
							"token",
						},
					},
				},
			},
		},
		{
			section: "hipchat",
			setDefaults: func(c *server.Config) {
				c.HipChat.URL = "http://hipchat.example.com"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/hipchat"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/hipchat/"},
					Options: map[string]interface{}{
						"enabled":            false,
						"global":             false,
						"room":               "",
						"state-changes-only": false,
						"token":              false,
						"url":                "http://hipchat.example.com",
					},
					Redacted: []string{
						"token",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/hipchat/"},
				Options: map[string]interface{}{
					"enabled":            false,
					"global":             false,
					"room":               "",
					"state-changes-only": false,
					"token":              false,
					"url":                "http://hipchat.example.com",
				},
				Redacted: []string{
					"token",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"token": "token",
							"room":  "kapacitor",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/hipchat"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/hipchat/"},
							Options: map[string]interface{}{
								"enabled":            false,
								"global":             false,
								"room":               "kapacitor",
								"state-changes-only": false,
								"token":              true,
								"url":                "http://hipchat.example.com",
							},
							Redacted: []string{
								"token",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/hipchat/"},
						Options: map[string]interface{}{
							"enabled":            false,
							"global":             false,
							"room":               "kapacitor",
							"state-changes-only": false,
							"token":              true,
							"url":                "http://hipchat.example.com",
						},
						Redacted: []string{
							"token",
						},
					},
				},
			},
		},
		{
			section: "mqtt",
			setDefaults: func(c *server.Config) {
				cfg := &mqtt.Config{
					Name: "default",
					URL:  "tcp://mqtt.example.com:1883",
				}
				cfg.SetNewClientF(mqtttest.NewClient)
				c.MQTT = mqtt.Configs{
					*cfg,
				}
			},
			element: "default",
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/mqtt"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/mqtt/default"},
					Options: map[string]interface{}{
						"enabled":              false,
						"name":                 "default",
						"default":              false,
						"url":                  "tcp://mqtt.example.com:1883",
						"ssl-ca":               "",
						"ssl-cert":             "",
						"ssl-key":              "",
						"insecure-skip-verify": false,
						"client-id":            "",
						"username":             "",
						"password":             false,
					},
					Redacted: []string{
						"password",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/mqtt/default"},
				Options: map[string]interface{}{
					"enabled":              false,
					"name":                 "default",
					"default":              false,
					"url":                  "tcp://mqtt.example.com:1883",
					"ssl-ca":               "",
					"ssl-cert":             "",
					"ssl-key":              "",
					"insecure-skip-verify": false,
					"client-id":            "",
					"username":             "",
					"password":             false,
				},
				Redacted: []string{
					"password",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"client-id": "kapacitor-default",
							"password":  "super secret",
						},
					},
					element: "default",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/mqtt"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/mqtt/default"},
							Options: map[string]interface{}{
								"enabled":              false,
								"name":                 "default",
								"default":              false,
								"url":                  "tcp://mqtt.example.com:1883",
								"ssl-ca":               "",
								"ssl-cert":             "",
								"ssl-key":              "",
								"insecure-skip-verify": false,
								"client-id":            "kapacitor-default",
								"username":             "",
								"password":             true,
							},
							Redacted: []string{
								"password",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/mqtt/default"},
						Options: map[string]interface{}{
							"enabled":              false,
							"name":                 "default",
							"default":              false,
							"url":                  "tcp://mqtt.example.com:1883",
							"ssl-ca":               "",
							"ssl-cert":             "",
							"ssl-key":              "",
							"insecure-skip-verify": false,
							"client-id":            "kapacitor-default",
							"username":             "",
							"password":             true,
						},
						Redacted: []string{
							"password",
						},
					},
				},
			},
		},
		{
			section: "opsgenie",
			setDefaults: func(c *server.Config) {
				c.OpsGenie.URL = "http://opsgenie.example.com"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie/"},
					Options: map[string]interface{}{
						"api-key":      false,
						"enabled":      false,
						"global":       false,
						"recipients":   nil,
						"recovery_url": opsgenie.DefaultOpsGenieRecoveryURL,
						"teams":        nil,
						"url":          "http://opsgenie.example.com",
					},
					Redacted: []string{
						"api-key",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie/"},
				Options: map[string]interface{}{
					"api-key":      false,
					"enabled":      false,
					"global":       false,
					"recipients":   nil,
					"recovery_url": opsgenie.DefaultOpsGenieRecoveryURL,
					"teams":        nil,
					"url":          "http://opsgenie.example.com",
				},
				Redacted: []string{
					"api-key",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"api-key": "token",
							"global":  true,
							"teams":   []string{"teamA", "teamB"},
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie/"},
							Options: map[string]interface{}{
								"api-key":      true,
								"enabled":      false,
								"global":       true,
								"recipients":   nil,
								"recovery_url": opsgenie.DefaultOpsGenieRecoveryURL,
								"teams":        []interface{}{"teamA", "teamB"},
								"url":          "http://opsgenie.example.com",
							},
							Redacted: []string{
								"api-key",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie/"},
						Options: map[string]interface{}{
							"api-key":      true,
							"enabled":      false,
							"global":       true,
							"recipients":   nil,
							"recovery_url": opsgenie.DefaultOpsGenieRecoveryURL,
							"teams":        []interface{}{"teamA", "teamB"},
							"url":          "http://opsgenie.example.com",
						},
						Redacted: []string{
							"api-key",
						},
					},
				},
			},
		},
		{
			section: "opsgenie2",
			setDefaults: func(c *server.Config) {
				c.OpsGenie2.URL = "http://opsgenie2.example.com"
				c.OpsGenie2.RecoveryAction = "notes"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie2"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie2/"},
					Options: map[string]interface{}{
						"api-key":         false,
						"enabled":         false,
						"global":          false,
						"recipients":      nil,
						"teams":           nil,
						"url":             "http://opsgenie2.example.com",
						"recovery_action": "notes",
					},
					Redacted: []string{
						"api-key",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie2/"},
				Options: map[string]interface{}{
					"api-key":         false,
					"enabled":         false,
					"global":          false,
					"recipients":      nil,
					"teams":           nil,
					"url":             "http://opsgenie2.example.com",
					"recovery_action": "notes",
				},
				Redacted: []string{
					"api-key",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"api-key": "token",
							"global":  true,
							"teams":   []string{"teamA", "teamB"},
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie2"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie2/"},
							Options: map[string]interface{}{
								"api-key":         true,
								"enabled":         false,
								"global":          true,
								"recipients":      nil,
								"teams":           []interface{}{"teamA", "teamB"},
								"url":             "http://opsgenie2.example.com",
								"recovery_action": "notes",
							},
							Redacted: []string{
								"api-key",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie2/"},
						Options: map[string]interface{}{
							"api-key":         true,
							"enabled":         false,
							"global":          true,
							"recipients":      nil,
							"teams":           []interface{}{"teamA", "teamB"},
							"url":             "http://opsgenie2.example.com",
							"recovery_action": "notes",
						},
						Redacted: []string{
							"api-key",
						},
					},
				},
			},
		},
		{
			section: "pagerduty",
			setDefaults: func(c *server.Config) {
				c.PagerDuty.ServiceKey = "secret"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty/"},
					Options: map[string]interface{}{
						"enabled":     false,
						"global":      false,
						"service-key": true,
						"url":         pagerduty.DefaultPagerDutyAPIURL,
					},
					Redacted: []string{
						"service-key",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty/"},
				Options: map[string]interface{}{
					"enabled":     false,
					"global":      false,
					"service-key": true,
					"url":         pagerduty.DefaultPagerDutyAPIURL,
				},
				Redacted: []string{
					"service-key",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"service-key": "",
							"enabled":     true,
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty/"},
							Options: map[string]interface{}{
								"enabled":     true,
								"global":      false,
								"service-key": false,
								"url":         pagerduty.DefaultPagerDutyAPIURL,
							},
							Redacted: []string{
								"service-key",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty/"},
						Options: map[string]interface{}{
							"enabled":     true,
							"global":      false,
							"service-key": false,
							"url":         pagerduty.DefaultPagerDutyAPIURL,
						},
						Redacted: []string{
							"service-key",
						},
					},
				},
			},
		},
		{
			section: "pagerduty2",
			setDefaults: func(c *server.Config) {
				c.PagerDuty2.RoutingKey = "secret"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty2"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty2/"},
					Options: map[string]interface{}{
						"enabled":     false,
						"global":      false,
						"routing-key": true,
						"url":         pagerduty2.DefaultPagerDuty2APIURL,
					},
					Redacted: []string{
						"routing-key",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty2/"},
				Options: map[string]interface{}{
					"enabled":     false,
					"global":      false,
					"routing-key": true,
					"url":         pagerduty2.DefaultPagerDuty2APIURL,
				},
				Redacted: []string{
					"routing-key",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"routing-key": "",
							"enabled":     true,
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty2"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty2/"},
							Options: map[string]interface{}{
								"enabled":     true,
								"global":      false,
								"routing-key": false,
								"url":         pagerduty2.DefaultPagerDuty2APIURL,
							},
							Redacted: []string{
								"routing-key",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty2/"},
						Options: map[string]interface{}{
							"enabled":     true,
							"global":      false,
							"routing-key": false,
							"url":         pagerduty2.DefaultPagerDuty2APIURL,
						},
						Redacted: []string{
							"routing-key",
						},
					},
				},
			},
		},
		{
			section: "smtp",
			setDefaults: func(c *server.Config) {
				c.SMTP.Host = "smtp.example.com"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/smtp"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/smtp/"},
					Options: map[string]interface{}{
						"enabled":            false,
						"from":               "",
						"global":             false,
						"host":               "smtp.example.com",
						"idle-timeout":       "30s",
						"no-verify":          false,
						"password":           false,
						"port":               float64(25),
						"state-changes-only": false,
						"to":                 nil,
						"username":           "",
					},
					Redacted: []string{
						"password",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/smtp/"},
				Options: map[string]interface{}{
					"enabled":            false,
					"from":               "",
					"global":             false,
					"host":               "smtp.example.com",
					"idle-timeout":       "30s",
					"no-verify":          false,
					"password":           false,
					"port":               float64(25),
					"state-changes-only": false,
					"to":                 nil,
					"username":           "",
				},
				Redacted: []string{
					"password",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"idle-timeout": "1m0s",
							"global":       true,
							"password":     "secret",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/smtp"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/smtp/"},
							Options: map[string]interface{}{
								"enabled":            false,
								"from":               "",
								"global":             true,
								"host":               "smtp.example.com",
								"idle-timeout":       "1m0s",
								"no-verify":          false,
								"password":           true,
								"port":               float64(25),
								"state-changes-only": false,
								"to":                 nil,
								"username":           "",
							},
							Redacted: []string{
								"password",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/smtp/"},
						Options: map[string]interface{}{
							"enabled":            false,
							"from":               "",
							"global":             true,
							"host":               "smtp.example.com",
							"idle-timeout":       "1m0s",
							"no-verify":          false,
							"password":           true,
							"port":               float64(25),
							"state-changes-only": false,
							"to":                 nil,
							"username":           "",
						},
						Redacted: []string{
							"password",
						},
					},
				},
			},
		},
		{
			section: "sensu",
			setDefaults: func(c *server.Config) {
				c.Sensu.Addr = "sensu.example.com:3000"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sensu"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sensu/"},
					Options: map[string]interface{}{
						"addr":     "sensu.example.com:3000",
						"enabled":  false,
						"source":   "Kapacitor",
						"handlers": nil,
					},
					Redacted: nil,
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sensu/"},
				Options: map[string]interface{}{
					"addr":     "sensu.example.com:3000",
					"enabled":  false,
					"source":   "Kapacitor",
					"handlers": nil,
				},
				Redacted: nil,
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"addr":    "sensu.local:3000",
							"enabled": true,
							"source":  "Kapacitor",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sensu"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sensu/"},
							Options: map[string]interface{}{
								"addr":     "sensu.local:3000",
								"enabled":  true,
								"source":   "Kapacitor",
								"handlers": nil,
							},
							Redacted: nil,
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sensu/"},
						Options: map[string]interface{}{
							"addr":     "sensu.local:3000",
							"enabled":  true,
							"source":   "Kapacitor",
							"handlers": nil,
						},
						Redacted: nil,
					},
				},
			},
		},
		{
			section: "slack",
			setDefaults: func(c *server.Config) {
				cfg := &slack.Config{
					Global:   true,
					Default:  true,
					Username: slack.DefaultUsername,
				}

				c.Slack = slack.Configs{
					*cfg,
				}
			},
			element: "",
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/"},
					Options: map[string]interface{}{
						"workspace":            "",
						"default":              true,
						"channel":              "",
						"enabled":              false,
						"global":               true,
						"icon-emoji":           "",
						"state-changes-only":   false,
						"url":                  false,
						"username":             "kapacitor",
						"ssl-ca":               "",
						"ssl-cert":             "",
						"ssl-key":              "",
						"insecure-skip-verify": false,
					},
					Redacted: []string{
						"url",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/"},
				Options: map[string]interface{}{
					"workspace":            "",
					"default":              true,
					"channel":              "",
					"enabled":              false,
					"global":               true,
					"icon-emoji":           "",
					"state-changes-only":   false,
					"url":                  false,
					"username":             "kapacitor",
					"ssl-ca":               "",
					"ssl-cert":             "",
					"ssl-key":              "",
					"insecure-skip-verify": false,
				},
				Redacted: []string{
					"url",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Add: map[string]interface{}{
							"workspace": "company_private",
							"enabled":   true,
							"global":    false,
							"channel":   "#general",
							"username":  slack.DefaultUsername,
							"url":       "http://slack.example.com/secret-token",
						},
					},
					element: "company_private",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/"},
							Options: map[string]interface{}{
								"workspace":            "",
								"default":              true,
								"channel":              "",
								"enabled":              false,
								"global":               true,
								"icon-emoji":           "",
								"state-changes-only":   false,
								"url":                  false,
								"username":             "kapacitor",
								"ssl-ca":               "",
								"ssl-cert":             "",
								"ssl-key":              "",
								"insecure-skip-verify": false,
							},
							Redacted: []string{
								"url",
							},
						},
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_private"},
								Options: map[string]interface{}{
									"workspace":            "company_private",
									"channel":              "#general",
									"default":              false,
									"enabled":              true,
									"global":               false,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  true,
									"username":             "kapacitor",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
								},
							}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_private"},
						Options: map[string]interface{}{
							"workspace":            "company_private",
							"channel":              "#general",
							"default":              false,
							"enabled":              true,
							"global":               false,
							"icon-emoji":           "",
							"state-changes-only":   false,
							"url":                  true,
							"username":             "kapacitor",
							"ssl-ca":               "",
							"ssl-cert":             "",
							"ssl-key":              "",
							"insecure-skip-verify": false,
						},
						Redacted: []string{
							"url",
						},
					},
				},
				{
					updateAction: client.ConfigUpdateAction{
						Add: map[string]interface{}{
							"workspace": "company_public",
							"enabled":   true,
							"global":    false,
							"channel":   "#general",
							"username":  slack.DefaultUsername,
							"url":       "http://slack.example.com/secret-token",
						},
					},
					element: "company_public",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack"},
						Elements: []client.ConfigElement{
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/"},
								Options: map[string]interface{}{
									"workspace":            "",
									"default":              true,
									"channel":              "",
									"enabled":              false,
									"global":               true,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  false,
									"username":             "kapacitor",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
								},
							},
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_private"},
								Options: map[string]interface{}{
									"workspace":            "company_private",
									"channel":              "#general",
									"default":              false,
									"enabled":              true,
									"global":               false,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  true,
									"username":             "kapacitor",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
								},
							},
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_public"},
								Options: map[string]interface{}{
									"workspace":            "company_public",
									"channel":              "#general",
									"default":              false,
									"enabled":              true,
									"global":               false,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  true,
									"username":             "kapacitor",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
								},
							},
						},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_public"},
						Options: map[string]interface{}{
							"workspace":            "company_public",
							"channel":              "#general",
							"default":              false,
							"enabled":              true,
							"global":               false,
							"icon-emoji":           "",
							"state-changes-only":   false,
							"url":                  true,
							"username":             "kapacitor",
							"ssl-ca":               "",
							"ssl-cert":             "",
							"ssl-key":              "",
							"insecure-skip-verify": false,
						},
						Redacted: []string{
							"url",
						},
					},
				},
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"enabled":  false,
							"username": "testbot",
						},
					},
					element: "company_public",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack"},
						Elements: []client.ConfigElement{
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/"},
								Options: map[string]interface{}{
									"workspace":            "",
									"default":              true,
									"channel":              "",
									"enabled":              false,
									"global":               true,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  false,
									"username":             "kapacitor",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
								},
							},
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_private"},
								Options: map[string]interface{}{
									"workspace":            "company_private",
									"channel":              "#general",
									"default":              false,
									"enabled":              true,
									"global":               false,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  true,
									"username":             "kapacitor",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
								},
							},
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_public"},
								Options: map[string]interface{}{
									"workspace":            "company_public",
									"channel":              "#general",
									"default":              false,
									"enabled":              false,
									"global":               false,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  true,
									"username":             "testbot",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
								},
							},
						},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_public"},
						Options: map[string]interface{}{
							"workspace":            "company_public",
							"channel":              "#general",
							"default":              false,
							"enabled":              false,
							"global":               false,
							"icon-emoji":           "",
							"state-changes-only":   false,
							"url":                  true,
							"username":             "testbot",
							"ssl-ca":               "",
							"ssl-cert":             "",
							"ssl-key":              "",
							"insecure-skip-verify": false,
						},
						Redacted: []string{
							"url",
						},
					},
				},
				{
					updateAction: client.ConfigUpdateAction{
						Delete: []string{"username"},
					},
					element: "company_public",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack"},
						Elements: []client.ConfigElement{
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/"},
								Options: map[string]interface{}{
									"workspace":            "",
									"default":              true,
									"channel":              "",
									"enabled":              false,
									"global":               true,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  false,
									"username":             "kapacitor",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
								},
							},
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_private"},
								Options: map[string]interface{}{
									"workspace":            "company_private",
									"channel":              "#general",
									"default":              false,
									"enabled":              true,
									"global":               false,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  true,
									"username":             "kapacitor",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
								},
							},
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_public"},
								Options: map[string]interface{}{
									"workspace":            "company_public",
									"channel":              "#general",
									"default":              false,
									"enabled":              false,
									"global":               false,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  true,
									"username":             "",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
								},
							},
						},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_public"},
						Options: map[string]interface{}{
							"workspace":            "company_public",
							"channel":              "#general",
							"default":              false,
							"enabled":              false,
							"global":               false,
							"icon-emoji":           "",
							"state-changes-only":   false,
							"url":                  true,
							"username":             "",
							"ssl-ca":               "",
							"ssl-cert":             "",
							"ssl-key":              "",
							"insecure-skip-verify": false,
						},
						Redacted: []string{
							"url",
						},
					},
				},
			},
		},
		{
			section: "snmptrap",
			setDefaults: func(c *server.Config) {
				c.SNMPTrap.Community = "test"
				c.SNMPTrap.Retries = 2.0
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/snmptrap"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/snmptrap/"},
					Options: map[string]interface{}{
						"addr":      "localhost:162",
						"enabled":   false,
						"community": true,
						"retries":   2.0,
					},
					Redacted: []string{
						"community",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/snmptrap/"},
				Options: map[string]interface{}{
					"addr":      "localhost:162",
					"enabled":   false,
					"community": true,
					"retries":   2.0,
				},
				Redacted: []string{
					"community",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"enabled":   true,
							"addr":      "snmptrap.example.com:162",
							"community": "public",
							"retries":   1.0,
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/snmptrap"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/snmptrap/"},
							Options: map[string]interface{}{
								"addr":      "snmptrap.example.com:162",
								"enabled":   true,
								"community": true,
								"retries":   1.0,
							},
							Redacted: []string{
								"community",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/snmptrap/"},
						Options: map[string]interface{}{
							"addr":      "snmptrap.example.com:162",
							"enabled":   true,
							"community": true,
							"retries":   1.0,
						},
						Redacted: []string{
							"community",
						},
					},
				},
			},
		},
		{
			section: "swarm",
			setDefaults: func(c *server.Config) {
				c.Swarm = swarm.Configs{swarm.Config{
					Servers: []string{"http://localhost:80001"},
				}}
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/swarm"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/swarm/"},
					Options: map[string]interface{}{
						"id":                   "",
						"enabled":              false,
						"servers":              []interface{}{"http://localhost:80001"},
						"ssl-ca":               "",
						"ssl-cert":             "",
						"ssl-key":              "",
						"insecure-skip-verify": false,
					},
					Redacted: nil,
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/swarm/"},
				Options: map[string]interface{}{
					"id":                   "",
					"enabled":              false,
					"servers":              []interface{}{"http://localhost:80001"},
					"ssl-ca":               "",
					"ssl-cert":             "",
					"ssl-key":              "",
					"insecure-skip-verify": false,
				},
				Redacted: nil,
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"enabled": true,
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/swarm"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/swarm/"},
							Options: map[string]interface{}{
								"id":                   "",
								"enabled":              true,
								"servers":              []interface{}{"http://localhost:80001"},
								"ssl-ca":               "",
								"ssl-cert":             "",
								"ssl-key":              "",
								"insecure-skip-verify": false,
							},
							Redacted: nil,
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/swarm/"},
						Options: map[string]interface{}{
							"id":                   "",
							"enabled":              true,
							"servers":              []interface{}{"http://localhost:80001"},
							"ssl-ca":               "",
							"ssl-cert":             "",
							"ssl-key":              "",
							"insecure-skip-verify": false,
						},
						Redacted: nil,
					},
				},
			},
		},
		{
			section: "talk",
			setDefaults: func(c *server.Config) {
				c.Talk.AuthorName = "Kapacitor"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/talk"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/talk/"},
					Options: map[string]interface{}{
						"enabled":     false,
						"url":         false,
						"author_name": "Kapacitor",
					},
					Redacted: []string{
						"url",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/talk/"},
				Options: map[string]interface{}{
					"enabled":     false,
					"url":         false,
					"author_name": "Kapacitor",
				},
				Redacted: []string{
					"url",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"enabled": true,
							"url":     "http://talk.example.com/secret-token",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/talk"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/talk/"},
							Options: map[string]interface{}{
								"enabled":     true,
								"url":         true,
								"author_name": "Kapacitor",
							},
							Redacted: []string{
								"url",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/talk/"},
						Options: map[string]interface{}{
							"enabled":     true,
							"url":         true,
							"author_name": "Kapacitor",
						},
						Redacted: []string{
							"url",
						},
					},
				},
			},
		},
		{
			section: "telegram",
			setDefaults: func(c *server.Config) {
				c.Telegram.ChatId = "kapacitor"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/telegram"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/telegram/"},
					Options: map[string]interface{}{
						"chat-id":                  "kapacitor",
						"disable-notification":     false,
						"disable-web-page-preview": false,
						"enabled":                  false,
						"global":                   false,
						"parse-mode":               "",
						"state-changes-only":       false,
						"token":                    false,
						"url":                      telegram.DefaultTelegramURL,
					},
					Redacted: []string{
						"token",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/telegram/"},
				Options: map[string]interface{}{
					"chat-id":                  "kapacitor",
					"disable-notification":     false,
					"disable-web-page-preview": false,
					"enabled":                  false,
					"global":                   false,
					"parse-mode":               "",
					"state-changes-only":       false,
					"token":                    false,
					"url":                      telegram.DefaultTelegramURL,
				},
				Redacted: []string{
					"token",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"enabled": true,
							"token":   "token",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/telegram"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/telegram/"},
							Options: map[string]interface{}{
								"chat-id":                  "kapacitor",
								"disable-notification":     false,
								"disable-web-page-preview": false,
								"enabled":                  true,
								"global":                   false,
								"parse-mode":               "",
								"state-changes-only":       false,
								"token":                    true,
								"url":                      telegram.DefaultTelegramURL,
							},
							Redacted: []string{
								"token",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/telegram/"},
						Options: map[string]interface{}{
							"chat-id":                  "kapacitor",
							"disable-notification":     false,
							"disable-web-page-preview": false,
							"enabled":                  true,
							"global":                   false,
							"parse-mode":               "",
							"state-changes-only":       false,
							"token":                    true,
							"url":                      telegram.DefaultTelegramURL,
						},
						Redacted: []string{
							"token",
						},
					},
				},
			},
		},
		{
			section: "victorops",
			setDefaults: func(c *server.Config) {
				c.VictorOps.RoutingKey = "test"
				c.VictorOps.APIKey = "secret"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/victorops"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/victorops/"},
					Options: map[string]interface{}{
						"api-key":     true,
						"enabled":     false,
						"global":      false,
						"routing-key": "test",
						"url":         victorops.DefaultVictorOpsAPIURL,
						"json-data":   false,
					},
					Redacted: []string{
						"api-key",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/victorops/"},
				Options: map[string]interface{}{
					"api-key":     true,
					"enabled":     false,
					"global":      false,
					"routing-key": "test",
					"url":         victorops.DefaultVictorOpsAPIURL,
					"json-data":   false,
				},
				Redacted: []string{
					"api-key",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"api-key":   "",
							"global":    true,
							"json-data": true,
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/victorops"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/victorops/"},
							Options: map[string]interface{}{
								"api-key":     false,
								"enabled":     false,
								"global":      true,
								"routing-key": "test",
								"url":         victorops.DefaultVictorOpsAPIURL,
								"json-data":   true,
							},
							Redacted: []string{
								"api-key",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/victorops/"},
						Options: map[string]interface{}{
							"api-key":     false,
							"enabled":     false,
							"global":      true,
							"routing-key": "test",
							"url":         victorops.DefaultVictorOpsAPIURL,
							"json-data":   true,
						},
						Redacted: []string{
							"api-key",
						},
					},
				},
			},
		},
	}

	compareElements := func(got, exp client.ConfigElement) error {
		if got.Link != exp.Link {
			return fmt.Errorf("elements have different links, got %v exp %v", got.Link, exp.Link)
		}
		for k, v := range exp.Options {
			if g, ok := got.Options[k]; !ok {
				return fmt.Errorf("missing option %q", k)
			} else if !reflect.DeepEqual(g, v) {
				return fmt.Errorf("unexpected config option %q got %#v exp %#v types: got %T exp %T", k, g, v, g, v)
			}
		}
		for k := range got.Options {
			if v, ok := exp.Options[k]; !ok {
				return fmt.Errorf("extra option %q with value %#v", k, v)
			}
		}
		if len(got.Redacted) != len(exp.Redacted) {
			return fmt.Errorf("unexpected element redacted lists: got %v exp %v", got.Redacted, exp.Redacted)
		}
		sort.Strings(got.Redacted)
		sort.Strings(exp.Redacted)
		for i := range exp.Redacted {
			if got.Redacted[i] != exp.Redacted[i] {
				return fmt.Errorf("unexpected element redacted lists: got %v exp %v", got.Redacted, exp.Redacted)
			}
		}
		return nil
	}
	compareSections := func(got, exp client.ConfigSection) error {
		if got.Link != exp.Link {
			return fmt.Errorf("sections have different links, got %v exp %v", got.Link, exp.Link)
		}
		if len(got.Elements) != len(exp.Elements) {
			return fmt.Errorf("sections are different lengths, got %d exp %d", len(got.Elements), len(exp.Elements))
		}
		for i := range exp.Elements {
			if err := compareElements(got.Elements[i], exp.Elements[i]); err != nil {
				return errors.Wrapf(err, "section element %d are not equal", i)
			}
		}
		return nil
	}

	validate := func(
		cli *client.Client,
		section,
		element string,
		expSection client.ConfigSection,
		expElement client.ConfigElement,
	) error {
		// Get all sections
		if config, err := cli.ConfigSections(); err != nil {
			return errors.Wrap(err, "failed to get sections")
		} else {
			if err := compareSections(config.Sections[section], expSection); err != nil {
				return fmt.Errorf("%s: %v", section, err)
			}
		}
		// Get the specific section
		sectionLink := cli.ConfigSectionLink(section)
		if got, err := cli.ConfigSection(sectionLink); err != nil {
			return err
		} else {
			if err := compareSections(got, expSection); err != nil {
				return fmt.Errorf("%s: %v", section, err)
			}
		}
		elementLink := cli.ConfigElementLink(section, element)
		// Get the specific element
		if got, err := cli.ConfigElement(elementLink); err != nil {
			return err
		} else {
			if err := compareElements(got, expElement); err != nil {
				return fmt.Errorf("%s/%s: %v", section, element, err)
			}
		}
		return nil
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%s/%s-%d", tc.section, tc.element, i), func(t *testing.T) {
			// Create default config
			c := NewConfig()
			if tc.setDefaults != nil {
				tc.setDefaults(c)
			}
			s := OpenServer(c)
			cli := Client(s)
			defer s.Close()

			if err := validate(cli, tc.section, tc.element, tc.expDefaultSection, tc.expDefaultElement); err != nil {
				t.Errorf("unexpected defaults for %s/%s: %v", tc.section, tc.element, err)
			}

			for i, ua := range tc.updates {
				link := cli.ConfigElementLink(tc.section, ua.element)

				if len(ua.updateAction.Add) > 0 ||
					len(ua.updateAction.Remove) > 0 {
					link = cli.ConfigSectionLink(tc.section)
				}

				if err := cli.ConfigUpdate(link, ua.updateAction); err != nil {
					t.Fatal(err)
				}
				if err := validate(cli, tc.section, ua.element, ua.expSection, ua.expElement); err != nil {
					t.Errorf("unexpected update result %d for %s/%s: %v", i, tc.section, ua.element, err)
				}
			}
		})
	}
}
func TestServer_ListServiceTests(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()
	serviceTests, err := cli.ListServiceTests(nil)
	if err != nil {
		t.Fatal(err)
	}
	expServiceTests := client.ServiceTests{
		Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests"},
		Services: []client.ServiceTest{
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/alerta"},
				Name: "alerta",
				Options: client.ServiceTestOptions{
					"resource":    "testResource",
					"event":       "testEvent",
					"environment": "",
					"severity":    "critical",
					"group":       "testGroup",
					"value":       "testValue",
					"message":     "test alerta message",
					"origin":      "",
					"service": []interface{}{
						"testServiceA",
						"testServiceB",
					},
					"timeout": "24h0m0s",
				},
			},
			{
				Link: client.Link{Relation: "self", Href: "/kapacitor/v1/service-tests/azure"},
				Name: "azure",
				Options: client.ServiceTestOptions{
					"id": "",
				},
			},
			{
				Link: client.Link{Relation: "self", Href: "/kapacitor/v1/service-tests/consul"},
				Name: "consul",
				Options: client.ServiceTestOptions{
					"id": "",
				},
			},
			{
				Link: client.Link{Relation: "self", Href: "/kapacitor/v1/service-tests/dns"},
				Name: "dns",
				Options: client.ServiceTestOptions{
					"id": ""},
			},
			{
				Link: client.Link{Relation: "self", Href: "/kapacitor/v1/service-tests/ec2"},
				Name: "ec2",
				Options: client.ServiceTestOptions{
					"id": "",
				},
			},
			{
				Link: client.Link{Relation: "self", Href: "/kapacitor/v1/service-tests/file-discovery"},
				Name: "file-discovery",
				Options: client.ServiceTestOptions{
					"id": "",
				},
			},
			{
				Link: client.Link{Relation: "self", Href: "/kapacitor/v1/service-tests/gce"},
				Name: "gce",
				Options: client.ServiceTestOptions{
					"id": "",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/hipchat"},
				Name: "hipchat",
				Options: client.ServiceTestOptions{
					"room":    "",
					"message": "test hipchat message",
					"level":   "CRITICAL",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/httppost"},
				Name: "httppost",
				Options: client.ServiceTestOptions{
					"endpoint": "example",
					"url":      "http://localhost:3000/",
					"headers":  map[string]interface{}{"Auth": "secret"},
					"timeout":  float64(0),
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/influxdb"},
				Name: "influxdb",
				Options: client.ServiceTestOptions{
					"cluster": "",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/kafka"},
				Name: "kafka",
				Options: client.ServiceTestOptions{
					"cluster": "example",
					"topic":   "test",
					"key":     "key",
					"message": "test kafka message",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/kubernetes"},
				Name: "kubernetes",
				Options: client.ServiceTestOptions{
					"id": "",
				},
			},
			{
				Link: client.Link{Relation: "self", Href: "/kapacitor/v1/service-tests/marathon"},
				Name: "marathon",
				Options: client.ServiceTestOptions{
					"id": "",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/mqtt"},
				Name: "mqtt",
				Options: client.ServiceTestOptions{
					"broker-name": "",
					"topic":       "",
					"message":     "test MQTT message",
					"qos":         "at-most-once",
					"retained":    false,
				},
			},
			{
				Link: client.Link{Relation: "self", Href: "/kapacitor/v1/service-tests/nerve"},
				Name: "nerve",
				Options: client.ServiceTestOptions{
					"id": "",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/opsgenie"},
				Name: "opsgenie",
				Options: client.ServiceTestOptions{
					"teams":        nil,
					"recipients":   nil,
					"message-type": "CRITICAL",
					"message":      "test opsgenie message",
					"entity-id":    "testEntityID",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/opsgenie2"},
				Name: "opsgenie2",
				Options: client.ServiceTestOptions{
					"teams":        nil,
					"recipients":   nil,
					"message-type": "CRITICAL",
					"message":      "test opsgenie message",
					"entity-id":    "testEntityID",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/pagerduty"},
				Name: "pagerduty",
				Options: client.ServiceTestOptions{
					"incident-key": "testIncidentKey",
					"description":  "test pagerduty message",
					"level":        "CRITICAL",
					"details":      "",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/pagerduty2"},
				Name: "pagerduty2",
				Options: client.ServiceTestOptions{
					"alert_id":    "testAlertID",
					"description": "test pagerduty2 message",
					"level":       "CRITICAL",
					"event_data": map[string]interface{}{
						"Fields": map[string]interface{}{},
						"Result": map[string]interface{}{
							"series": interface{}(nil),
						},
						"Name":        "testPagerDuty2",
						"TaskName":    "",
						"Group":       "",
						"Tags":        map[string]interface{}{},
						"Recoverable": false,
						"Category":    "",
					},
					"timestamp": "2014-11-12T11:45:26.371Z",
					"links": []interface{}{
						map[string]interface{}{
							"href": "https://example.com/a",
							"text": "a",
						},
						map[string]interface{}{
							"href": "https://example.com/b",
							"text": "b",
						},
					},
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/pushover"},
				Name: "pushover",
				Options: client.ServiceTestOptions{
					"user-key":  "", //gohere
					"message":   "test pushover message",
					"device":    "",
					"title":     "",
					"url":       "",
					"url-title": "",
					"sound":     "",
					"level":     "CRITICAL",
				},
			},
			{
				Link: client.Link{Relation: "self", Href: "/kapacitor/v1/service-tests/scraper"},
				Name: "scraper",
				Options: client.ServiceTestOptions{
					"name": "",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/sensu"},
				Name: "sensu",
				Options: client.ServiceTestOptions{
					"name":     "testName",
					"output":   "testOutput",
					"source":   "Kapacitor",
					"handlers": []interface{}{},
					"metadata": map[string]interface{}{},
					"level":    "CRITICAL",
				},
			},
			{
				Link: client.Link{Relation: "self", Href: "/kapacitor/v1/service-tests/serverset"},
				Name: "serverset",
				Options: client.ServiceTestOptions{
					"id": "",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/slack"},
				Name: "slack",
				Options: client.ServiceTestOptions{
					"workspace":  "",
					"channel":    "",
					"icon-emoji": "",
					"level":      "CRITICAL",
					"message":    "test slack message",
					"username":   "",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/smtp"},
				Name: "smtp",
				Options: client.ServiceTestOptions{
					"to":      nil,
					"subject": "test subject",
					"body":    "test body",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/snmptrap"},
				Name: "snmptrap",
				Options: client.ServiceTestOptions{
					"trap-oid": "1.1.1.1",
					"data-list": []interface{}{
						map[string]interface{}{
							"oid":   "1.1.1.1.2",
							"type":  "s",
							"value": "test snmptrap message",
						},
					},
				},
			},
			{
				Link: client.Link{Relation: "self", Href: "/kapacitor/v1/service-tests/static-discovery"},
				Name: "static-discovery",
				Options: client.ServiceTestOptions{
					"id": "",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/swarm"},
				Name: "swarm",
				Options: client.ServiceTestOptions{
					"id": "",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/talk"},
				Name: "talk",
				Options: client.ServiceTestOptions{
					"title": "testTitle",
					"text":  "test talk text",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/telegram"},
				Name: "telegram",
				Options: client.ServiceTestOptions{
					"chat-id":                  "",
					"parse-mode":               "",
					"message":                  "test telegram message",
					"disable-web-page-preview": false,
					"disable-notification":     false,
				},
			},
			{
				Link: client.Link{Relation: "self", Href: "/kapacitor/v1/service-tests/triton"},
				Name: "triton",
				Options: client.ServiceTestOptions{
					"id": "",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/victorops"},
				Name: "victorops",
				Options: client.ServiceTestOptions{
					"routingKey":  "",
					"messageType": "CRITICAL",
					"message":     "test victorops message",
					"entityID":    "testEntityID",
				},
			},
		},
	}
	if got, exp := serviceTests.Link.Href, expServiceTests.Link.Href; got != exp {
		t.Errorf("unexpected service tests link.href: got %s exp %s", got, exp)
	}
	if got, exp := len(serviceTests.Services), len(expServiceTests.Services); got != exp {
		t.Fatalf("unexpected length of services: got %d exp %d", got, exp)
	}
	for i := range expServiceTests.Services {
		exp := expServiceTests.Services[i]
		got := serviceTests.Services[i]
		if !reflect.DeepEqual(got, exp) {
			t.Errorf("unexpected server test %s:\n%s", exp.Name, cmp.Diff(exp, got))
		}
	}
}

func TestServer_ListServiceTests_WithPattern(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()
	serviceTests, err := cli.ListServiceTests(&client.ListServiceTestsOptions{
		Pattern: "s*",
	})
	if err != nil {
		t.Fatal(err)
	}
	expServiceTests := client.ServiceTests{
		Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests"},
		Services: []client.ServiceTest{
			{
				Link: client.Link{Relation: "self", Href: "/kapacitor/v1/service-tests/scraper"},
				Name: "scraper",
				Options: client.ServiceTestOptions{
					"name": "",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/sensu"},
				Name: "sensu",
				Options: client.ServiceTestOptions{
					"name":     "testName",
					"output":   "testOutput",
					"source":   "Kapacitor",
					"handlers": []interface{}{},
					"metadata": map[string]interface{}{},
					"level":    "CRITICAL",
				},
			},
			{
				Link: client.Link{Relation: "self", Href: "/kapacitor/v1/service-tests/serverset"},
				Name: "serverset",
				Options: client.ServiceTestOptions{
					"id": "",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/slack"},
				Name: "slack",
				Options: client.ServiceTestOptions{
					"workspace":  "",
					"channel":    "",
					"icon-emoji": "",
					"level":      "CRITICAL",
					"message":    "test slack message",
					"username":   "",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/smtp"},
				Name: "smtp",
				Options: client.ServiceTestOptions{
					"to":      nil,
					"subject": "test subject",
					"body":    "test body",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/snmptrap"},
				Name: "snmptrap",
				Options: client.ServiceTestOptions{
					"trap-oid": "1.1.1.1",
					"data-list": []interface{}{
						map[string]interface{}{
							"oid":   "1.1.1.1.2",
							"type":  "s",
							"value": "test snmptrap message",
						},
					},
				},
			},
			{
				Link: client.Link{Relation: "self", Href: "/kapacitor/v1/service-tests/static-discovery"},
				Name: "static-discovery",
				Options: client.ServiceTestOptions{
					"id": "",
				},
			},
			{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/service-tests/swarm"},
				Name: "swarm",
				Options: client.ServiceTestOptions{
					"id": "",
				},
			},
		},
	}
	if got, exp := serviceTests.Link.Href, expServiceTests.Link.Href; got != exp {
		t.Errorf("unexpected service tests link.href: got %s exp %s", got, exp)
	}
	if got, exp := len(serviceTests.Services), len(expServiceTests.Services); got != exp {
		t.Fatalf("unexpected length of services: got %d exp %d", got, exp)

	}
	for i := range expServiceTests.Services {
		exp := expServiceTests.Services[i]
		got := serviceTests.Services[i]
		if !reflect.DeepEqual(got, exp) {
			t.Errorf("unexpected server test %s:\ngot\n%#v\nexp\n%#v\n", exp.Name, got, exp)
		}
	}
}

func TestServer_DoServiceTest(t *testing.T) {
	db := NewInfluxDB(func(q string) *iclient.Response {
		return &iclient.Response{}
	})
	testCases := []struct {
		service     string
		setDefaults func(*server.Config)
		options     client.ServiceTestOptions
		exp         client.ServiceTestResult
	}{
		{
			service: "alerta",
			options: client.ServiceTestOptions{},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "service is not enabled",
			},
		},
		{
			service: "hipchat",
			options: client.ServiceTestOptions{},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "service is not enabled",
			},
		},
		{
			service: "influxdb",
			setDefaults: func(c *server.Config) {
				c.InfluxDB[0].Enabled = true
				c.InfluxDB[0].Name = "default"
				c.InfluxDB[0].URLs = []string{db.URL()}
			},
			options: client.ServiceTestOptions{
				"cluster": "default",
			},
			exp: client.ServiceTestResult{
				Success: true,
				Message: "",
			},
		},
		{
			service: "influxdb",
			options: client.ServiceTestOptions{
				"cluster": "default",
			},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "cluster \"default\" is not enabled or does not exist",
			},
		},
		{
			service: "kubernetes",
			options: client.ServiceTestOptions{
				"id": "default",
			},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "unknown kubernetes cluster \"default\"",
			},
		},
		{
			service: "mqtt",
			options: client.ServiceTestOptions{
				"broker-name": "default",
				"topic":       "test",
			},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "unknown MQTT broker \"default\"",
			},
		},
		{
			service: "opsgenie",
			options: client.ServiceTestOptions{},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "service is not enabled",
			},
		},
		{
			service: "opsgenie2",
			options: client.ServiceTestOptions{},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "failed to prepare API request: service is not enabled",
			},
		},
		{
			service: "pagerduty",
			options: client.ServiceTestOptions{},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "service is not enabled",
			},
		},
		{
			service: "pagerduty2",
			options: client.ServiceTestOptions{},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "service is not enabled",
			},
		},
		{
			service: "pushover",
			options: client.ServiceTestOptions{},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "service is not enabled",
			},
		},
		{
			service: "sensu",
			options: client.ServiceTestOptions{},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "service is not enabled",
			},
		},
		{
			service: "slack",
			options: client.ServiceTestOptions{},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "service is not enabled",
			},
		},
		{
			service: "smtp",
			options: client.ServiceTestOptions{},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "service is not enabled",
			},
		},
		{
			service: "snmptrap",
			options: client.ServiceTestOptions{},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "service is not enabled",
			},
		},
		{
			service: "swarm",
			options: client.ServiceTestOptions{},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "unknown swarm cluster \"\"",
			},
		},
		{
			service: "talk",
			options: client.ServiceTestOptions{},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "service is not enabled",
			},
		},
		{
			service: "telegram",
			options: client.ServiceTestOptions{},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "service is not enabled",
			},
		},
		{
			service: "victorops",
			options: client.ServiceTestOptions{},
			exp: client.ServiceTestResult{
				Success: false,
				Message: "service is not enabled",
			},
		},
	}

	for _, tc := range testCases {
		// Create default config
		c := NewConfig()
		if tc.setDefaults != nil {
			tc.setDefaults(c)
		}
		s := OpenServer(c)
		cli := Client(s)
		defer s.Close()

		tr, err := cli.DoServiceTest(cli.ServiceTestLink(tc.service), tc.options)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(tr, tc.exp) {
			t.Log("Options", tc.options)
			t.Errorf("unexpected service test result for %s:\ngot\n%#v\nexp\n%#v\n", tc.service, tr, tc.exp)
		}
	}
}

func TestServer_AlertHandlers_CRUD(t *testing.T) {
	testCases := []struct {
		topic     string
		create    client.TopicHandlerOptions
		expCreate client.TopicHandler
		patch     client.JSONPatch
		expPatch  client.TopicHandler
		put       client.TopicHandlerOptions
		expPut    client.TopicHandler
	}{
		{
			topic: "system",
			create: client.TopicHandlerOptions{
				ID:   "myhandler",
				Kind: "slack",
				Options: map[string]interface{}{
					"channel": "#test",
				},
			},
			expCreate: client.TopicHandler{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/alerts/topics/system/handlers/myhandler"},
				ID:   "myhandler",
				Kind: "slack",
				Options: map[string]interface{}{
					"channel": "#test",
				},
			},
			patch: client.JSONPatch{
				{
					Path:      "/kind",
					Operation: "replace",
					Value:     "log",
				},
				{
					Path:      "/options/channel",
					Operation: "remove",
				},
				{
					Path:      "/options/path",
					Operation: "add",
					Value:     AlertLogPath,
				},
			},
			expPatch: client.TopicHandler{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/alerts/topics/system/handlers/myhandler"},
				ID:   "myhandler",
				Kind: "log",
				Options: map[string]interface{}{
					"path": AlertLogPath,
				},
			},
			put: client.TopicHandlerOptions{
				ID:   "newid",
				Kind: "smtp",
				Options: map[string]interface{}{
					"to": []string{"oncall@example.com"},
				},
			},
			expPut: client.TopicHandler{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/alerts/topics/system/handlers/newid"},
				ID:   "newid",
				Kind: "smtp",
				Options: map[string]interface{}{
					"to": []interface{}{"oncall@example.com"},
				},
			},
		},
		{
			// Topic and handler have same name
			topic: "slack",
			create: client.TopicHandlerOptions{
				ID:   "slack",
				Kind: "slack",
				Options: map[string]interface{}{
					"channel": "#test",
				},
			},
			expCreate: client.TopicHandler{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/alerts/topics/slack/handlers/slack"},
				ID:   "slack",
				Kind: "slack",
				Options: map[string]interface{}{
					"channel": "#test",
				},
			},
			patch: client.JSONPatch{
				{
					Path:      "/kind",
					Operation: "replace",
					Value:     "log",
				},
				{
					Path:      "/options/channel",
					Operation: "remove",
				},
				{
					Path:      "/options/path",
					Operation: "add",
					Value:     AlertLogPath,
				},
			},
			expPatch: client.TopicHandler{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/alerts/topics/slack/handlers/slack"},
				ID:   "slack",
				Kind: "log",
				Options: map[string]interface{}{
					"path": AlertLogPath,
				},
			},
			put: client.TopicHandlerOptions{
				ID:   "slack",
				Kind: "smtp",
				Options: map[string]interface{}{
					"to": []string{"oncall@example.com"},
				},
			},
			expPut: client.TopicHandler{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/alerts/topics/slack/handlers/slack"},
				ID:   "slack",
				Kind: "smtp",
				Options: map[string]interface{}{
					"to": []interface{}{"oncall@example.com"},
				},
			},
		},
	}
	for _, tc := range testCases {
		// Create default config
		c := NewConfig()
		s := OpenServer(c)
		cli := Client(s)
		defer s.Close()

		h, err := cli.CreateTopicHandler(cli.TopicHandlersLink(tc.topic), tc.create)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(h, tc.expCreate) {
			t.Errorf("unexpected handler created:\ngot\n%#v\nexp\n%#v\n", h, tc.expCreate)
		}

		h, err = cli.PatchTopicHandler(h.Link, tc.patch)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(h, tc.expPatch) {
			t.Errorf("unexpected handler patched:\ngot\n%#v\nexp\n%#v\n", h, tc.expPatch)
		}

		h, err = cli.ReplaceTopicHandler(h.Link, tc.put)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(h, tc.expPut) {
			t.Errorf("unexpected handler put:\ngot\n%#v\nexp\n%#v\n", h, tc.expPut)
		}

		// Restart server
		s.Restart()

		rh, err := cli.TopicHandler(h.Link)
		if err != nil {
			t.Fatalf("could not find handler after restart: %v", err)
		}
		if got, exp := rh, h; !reflect.DeepEqual(got, exp) {
			t.Errorf("unexpected handler after restart:\ngot\n%#v\nexp\n%#v\n", got, exp)
		}

		err = cli.DeleteTopicHandler(h.Link)
		if err != nil {
			t.Fatal(err)
		}

		_, err = cli.TopicHandler(h.Link)
		if err == nil {
			t.Errorf("expected handler to be deleted")
		}

		handlers, err := cli.ListTopicHandlers(cli.TopicHandlersLink(tc.topic), nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range handlers.Handlers {
			if h.ID == tc.expPut.ID {
				t.Errorf("expected handler to be deleted")
				break
			}
		}
	}
}

func TestServer_AlertHandlers(t *testing.T) {

	resultJSON := `{"series":[{"name":"alert","columns":["time","value"],"values":[["1970-01-01T00:00:00Z",1]]}]}`

	alertData := alert.Data{
		ID:          "id",
		Message:     "message",
		Details:     "details",
		Time:        time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		Level:       alert.Critical,
		Recoverable: true,
		Data: models.Result{
			Series: models.Rows{
				{
					Name:    "alert",
					Columns: []string{"time", "value"},
					Values: [][]interface{}{[]interface{}{
						time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
						1.0,
					}},
				},
			},
		},
	}
	adJSON, err := json.Marshal(alertData)
	if err != nil {
		t.Fatal(err)
	}
	testCases := []struct {
		handler client.TopicHandler
		setup   func(*server.Config, *client.TopicHandler) (context.Context, error)
		result  func(context.Context) error
	}{
		{
			handler: client.TopicHandler{
				Kind: "alerta",
				Options: map[string]interface{}{
					"token":        "testtoken1234567",
					"token-prefix": "Bearer",
					"origin":       "kapacitor",
					"group":        "test",
					"environment":  "env",
					"timeout":      time.Duration(24 * time.Hour),
				},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts := alertatest.NewServer()
				ctxt := context.WithValue(nil, "server", ts)

				c.Alerta.Enabled = true
				c.Alerta.URL = ts.URL
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*alertatest.Server)
				ts.Close()
				got := ts.Requests()
				exp := []alertatest.Request{{
					URL:           "/alert",
					Authorization: "Bearer testtoken1234567",
					PostData: alertatest.PostData{
						Resource:    "alert",
						Event:       "id",
						Group:       "test",
						Environment: "env",
						Text:        "message",
						Origin:      "kapacitor",
						Service:     []string{"alert"},
						Timeout:     86400,
					},
				}}
				if !reflect.DeepEqual(exp, got) {
					return fmt.Errorf("unexpected alerta request:\nexp\n%+v\ngot\n%+v\n", exp, got)
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "exec",
				Options: map[string]interface{}{
					"prog": "/bin/alert-handler.sh",
					"args": []string{"arg1", "arg2", "arg3"},
				},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				te := alerttest.NewExec()
				ctxt := context.WithValue(nil, "exec", te)
				c.Commander = te.Commander
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				te := ctxt.Value("exec").(*alerttest.Exec)
				expData := []*commandtest.Command{{
					Spec: command.Spec{
						Prog: "/bin/alert-handler.sh",
						Args: []string{"arg1", "arg2", "arg3"},
					},
					Started:   true,
					Waited:    true,
					Killed:    false,
					StdinData: append(adJSON, '\n'),
				}}
				cmds := te.Commands()
				if got, exp := len(cmds), len(expData); got != exp {
					return fmt.Errorf("unexpected commands length: got %d exp %d", got, exp)
				}
				for i := range expData {
					if err := expData[i].Compare(cmds[i]); err != nil {
						return fmt.Errorf("unexpected command %d: %v", i, err)
					}
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "hipchat",
				Options: map[string]interface{}{
					"token": "testtoken1234567",
					"room":  "1234567",
				},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts := hipchattest.NewServer()
				ctxt := context.WithValue(nil, "server", ts)

				c.HipChat.Enabled = true
				c.HipChat.URL = ts.URL
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*hipchattest.Server)
				ts.Close()
				got := ts.Requests()
				exp := []hipchattest.Request{{
					URL: "/1234567/notification?auth_token=testtoken1234567",
					PostData: hipchattest.PostData{
						From:    "kapacitor",
						Message: "message",
						Color:   "red",
						Notify:  true,
					},
				}}
				if !reflect.DeepEqual(exp, got) {
					return fmt.Errorf("unexpected hipchat request:\nexp\n%+v\ngot\n%+v\n", exp, got)
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "kafka",
				Options: map[string]interface{}{
					"cluster": "default",
					"topic":   "test",
				},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts, err := kafkatest.NewServer()
				if err != nil {
					return nil, err
				}
				ctxt := context.WithValue(nil, "server", ts)

				c.Kafka = kafka.Configs{{
					Enabled: true,
					ID:      "default",
					Brokers: []string{ts.Addr.String()},
				}}
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*kafkatest.Server)
				time.Sleep(2 * time.Second)
				ts.Close()
				got, err := ts.Messages()
				if err != nil {
					return err
				}
				exp := []kafkatest.Message{{
					Topic:     "test",
					Partition: 1,
					Offset:    0,
					Key:       "id",
					Message:   string(adJSON) + "\n",
				}}
				if !cmp.Equal(exp, got) {
					return fmt.Errorf("unexpected kafka messages -exp/+got:\n%s", cmp.Diff(exp, got))
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "log",
				Options: map[string]interface{}{
					"mode": 0604,
				},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				tdir := MustTempDir()
				p := filepath.Join(tdir, "alert.log")

				ha.Options["path"] = p

				l := alerttest.NewLog(p)

				ctxt := context.WithValue(nil, "tdir", tdir)
				ctxt = context.WithValue(ctxt, "log", l)
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				tdir := ctxt.Value("tdir").(string)
				defer os.RemoveAll(tdir)
				l := ctxt.Value("log").(*alerttest.Log)
				expData := []alert.Data{alertData}
				expMode := os.FileMode(LogFileExpectedMode)

				m, err := l.Mode()
				if err != nil {
					return err
				}
				if got, exp := m, expMode; exp != got {
					return fmt.Errorf("unexpected file mode: got %v exp %v", got, exp)
				}
				data, err := l.Data()
				if err != nil {
					return err
				}
				if got, exp := data, expData; !reflect.DeepEqual(got, exp) {
					return fmt.Errorf("unexpected alert data written to log:\ngot\n%+v\nexp\n%+v\n", got, exp)
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "mqtt",
				Options: map[string]interface{}{
					"topic":    "test",
					"qos":      "at-least-once",
					"retained": true,
				},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				cc := new(mqtttest.ClientCreator)
				ctxt := context.WithValue(nil, "clientCreator", cc)
				cfg := &mqtt.Config{
					Enabled: true,
					Name:    "test",
					URL:     "tcp://mqtt.example.com:1883",
				}

				cfg.SetNewClientF(cc.NewClient)

				c.MQTT = mqtt.Configs{*cfg}
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				s := ctxt.Value("clientCreator").(*mqtttest.ClientCreator)
				if got, exp := len(s.Clients), 1; got != exp {
					return fmt.Errorf("unexpected number of clients created : exp %d got: %d", exp, got)
				}
				if got, exp := len(s.Configs), 1; got != exp {
					return fmt.Errorf("unexpected number of configs received: exp %d got: %d", exp, got)
				}
				if got, exp := s.Configs[0].URL, "tcp://mqtt.example.com:1883"; exp != got {
					return fmt.Errorf("unexpected config URL: exp %q got %q", exp, got)
				}
				got := s.Clients[0].PublishData
				exp := []mqtttest.PublishData{{
					Topic:    "test",
					QoS:      mqtt.AtLeastOnce,
					Retained: true,
					Message:  []byte("message"),
				}}
				if !reflect.DeepEqual(exp, got) {
					return fmt.Errorf("unexpected mqtt publish data:\nexp\n%+v\ngot\n%+v\n", exp, got)
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "opsgenie",
				Options: map[string]interface{}{
					"teams-list":      []string{"A team", "B team"},
					"recipients-list": []string{"test_recipient1", "test_recipient2"},
				},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts := opsgenietest.NewServer()
				ctxt := context.WithValue(nil, "server", ts)

				c.OpsGenie.Enabled = true
				c.OpsGenie.URL = ts.URL
				c.OpsGenie.APIKey = "api_key"
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*opsgenietest.Server)
				ts.Close()
				got := ts.Requests()
				exp := []opsgenietest.Request{{
					URL: "/",
					PostData: opsgenietest.PostData{
						ApiKey:  "api_key",
						Message: "message",
						Entity:  "id",
						Alias:   "id",
						Note:    "",
						Details: map[string]interface{}{
							"Level":           "CRITICAL",
							"Monitoring Tool": "Kapacitor",
						},
						Description: resultJSON,
						Teams:       []string{"A team", "B team"},
						Recipients:  []string{"test_recipient1", "test_recipient2"},
					},
				}}
				if !reflect.DeepEqual(exp, got) {
					return fmt.Errorf("unexpected opsgenie request:\nexp\n%+v\ngot\n%+v\n", exp, got)
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "opsgenie2",
				Options: map[string]interface{}{
					"teams-list":      []string{"A team", "B team"},
					"recipients-list": []string{"test_recipient1", "test_recipient2"},
				},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts := opsgenie2test.NewServer()
				ctxt := context.WithValue(nil, "server", ts)

				c.OpsGenie2.Enabled = true
				c.OpsGenie2.URL = ts.URL
				c.OpsGenie2.RecoveryAction = "notes"
				c.OpsGenie2.APIKey = "api_key"
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*opsgenie2test.Server)
				ts.Close()
				got := ts.Requests()
				exp := []opsgenie2test.Request{{
					URL:           "/",
					Authorization: "GenieKey api_key",
					PostData: opsgenie2test.PostData{
						Message:  "message",
						Entity:   "id",
						Alias:    "aWQ=",
						Note:     "",
						Priority: "P1",
						Details: map[string]string{
							"Level":               "CRITICAL",
							"Monitoring Tool":     "Kapacitor",
							"Kapacitor Task Name": "alert",
						},
						Description: resultJSON,
						Responders: []map[string]string{
							{"name": "A team", "type": "team"},
							{"name": "B team", "type": "team"},
							{"username": "test_recipient1", "type": "user"},
							{"username": "test_recipient2", "type": "user"},
						},
					},
				}}
				if !reflect.DeepEqual(exp, got) {
					return fmt.Errorf("unexpected opsgenie2 request:\nexp\n%+v\ngot\n%+v\n", exp, got)
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "pagerduty",
				Options: map[string]interface{}{
					"service-key": "service_key",
				},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts := pagerdutytest.NewServer()
				ctxt := context.WithValue(nil, "server", ts)

				c.PagerDuty.Enabled = true
				c.PagerDuty.URL = ts.URL
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*pagerdutytest.Server)
				kapacitorURL := ctxt.Value("kapacitorURL").(string)
				ts.Close()
				got := ts.Requests()
				exp := []pagerdutytest.Request{{
					URL: "/",
					PostData: pagerdutytest.PostData{
						ServiceKey:  "service_key",
						EventType:   "trigger",
						Description: "message",
						Client:      "kapacitor",
						ClientURL:   kapacitorURL,
						Details:     "details",
					},
				}}
				if !reflect.DeepEqual(exp, got) {
					return fmt.Errorf("unexpected pagerduty request:\nexp\n%+v\ngot\n%+v\n", exp, got)
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "pagerduty2",
				Options: map[string]interface{}{
					"routing-key": "rkey",
					"links": []interface{}{
						map[string]string{
							"href": "http://example.com",
							"text": "t1",
						},
						map[string]string{
							"href": "http://example.com/{{.TaskName}}",
							"text": "t2",
						},
					},
				},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts := pagerduty2test.NewServer()
				ctxt := context.WithValue(nil, "server", ts)

				c.PagerDuty2.Enabled = true
				c.PagerDuty2.URL = ts.URL
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*pagerduty2test.Server)
				kapacitorURL := ctxt.Value("kapacitorURL").(string)
				ts.Close()
				got := ts.Requests()
				exp := []pagerduty2test.Request{{
					URL: "/",
					PostData: pagerduty2test.PostData{
						Client:      "kapacitor",
						ClientURL:   kapacitorURL,
						EventAction: "trigger",
						DedupKey:    "id",
						Payload: &pagerduty2test.PDCEF{
							Summary:  "message",
							Source:   "unknown",
							Severity: "critical",
							Class:    "testAlertHandlers",
							CustomDetails: map[string]interface{}{
								"result": map[string]interface{}{
									"series": []interface{}{
										map[string]interface{}{
											"name":    "alert",
											"columns": []interface{}{"time", "value"},
											"values": []interface{}{
												[]interface{}{"1970-01-01T00:00:00Z", float64(1)},
											},
										},
									},
								},
							},
							Timestamp: "1970-01-01T00:00:00.000000000Z",
						},
						RoutingKey: "rkey",
						Links: []pagerduty2test.Link{
							{Href: "http://example.com", Text: "t1"},
							{Href: "http://example.com/testAlertHandlers", Text: "t2"},
						},
					},
				}}

				if !reflect.DeepEqual(exp, got) {
					return fmt.Errorf("unexpected pagerduty2 request:\nexp\n%+v\ngot\n%+v\n", exp, got)
				}

				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "post",
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts := alerttest.NewPostServer()

				ha.Options = map[string]interface{}{"url": ts.URL}

				ctxt := context.WithValue(nil, "server", ts)
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*alerttest.PostServer)
				ts.Close()
				exp := []alert.Data{alertData}
				got := ts.Data()
				if !reflect.DeepEqual(exp, got) {
					return fmt.Errorf("unexpected post request:\nexp\n%+v\ngot\n%+v\n", exp, got)
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "post",
				Options: map[string]interface{}{
					"endpoint": "test",
				},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts := httpposttest.NewAlertServer(nil, true)
				ctxt := context.WithValue(nil, "server", ts)
				c.HTTPPost = httppost.Configs{{
					Endpoint:      "test",
					URL:           ts.URL,
					AlertTemplate: `{{.Message}}`,
				}}
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*httpposttest.AlertServer)
				exp := []httpposttest.AlertRequest{{
					MatchingHeaders: true,
					Raw:             []byte("message"),
				}}
				got := ts.Data()
				if !reflect.DeepEqual(exp, got) {
					return fmt.Errorf("unexpected httppost alert request:\nexp\n%+v\ngot\n%+v\n", exp, got)
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind:    "pushover",
				Options: map[string]interface{}{},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts := pushovertest.NewServer()
				ctxt := context.WithValue(nil, "server", ts)

				c.Pushover.Enabled = true
				c.Pushover.URL = ts.URL
				c.Pushover.Token = "api_key"
				c.Pushover.UserKey = "user"
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*pushovertest.Server)
				ts.Close()
				got := ts.Requests()
				exp := []pushovertest.Request{{
					PostData: pushovertest.PostData{
						Token:    "api_key",
						UserKey:  "user",
						Message:  "message",
						Priority: 1,
					},
				}}
				if !reflect.DeepEqual(exp, got) {
					return fmt.Errorf("unexpected pushover request:\nexp\n%+v\ngot\n%+v\n", exp, got)
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "sensu",
				Options: map[string]interface{}{
					"source": "Kapacitor",
					"metadata": map[string]interface{}{
						"k1": "v1",
						"k2": 5,
					},
				},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts, err := sensutest.NewServer()
				if err != nil {
					return nil, err
				}
				ctxt := context.WithValue(nil, "server", ts)

				c.Sensu.Enabled = true
				c.Sensu.Addr = ts.Addr
				c.Sensu.Source = "Kapacitor"
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*sensutest.Server)
				ts.Close()
				exp := []sensutest.Request{{
					Source: "Kapacitor",
					Output: "message",
					Name:   "id",
					Status: 2,
					Metadata: map[string]interface{}{
						"k1": "v1",
						"k2": float64(5),
					},
				}}
				got := ts.Requests()
				if !reflect.DeepEqual(exp, got) {
					return fmt.Errorf("unexpected sensu request:\nexp\n%+v\ngot\n%+v\n", exp, got)
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "slack",
				Options: map[string]interface{}{
					"channel": "#test",
				},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts := slacktest.NewServer()
				ctxt := context.WithValue(nil, "server", ts)

				c.Slack[0].Enabled = true
				c.Slack[0].URL = ts.URL + "/test/slack/url"
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*slacktest.Server)
				ts.Close()
				got := ts.Requests()
				exp := []slacktest.Request{{
					URL: "/test/slack/url",
					PostData: slacktest.PostData{
						Channel:  "#test",
						Username: "kapacitor",
						Text:     "",
						Attachments: []slacktest.Attachment{
							{
								Fallback:  "message",
								Color:     "danger",
								Text:      "message",
								Mrkdwn_in: []string{"text"},
							},
						},
					},
				}}
				if !reflect.DeepEqual(exp, got) {
					return fmt.Errorf("unexpected slack request:\nexp\n%+v\ngot\n%+v\n", exp, got)
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "smtp",
				Options: map[string]interface{}{
					"to": []string{"oncall@example.com", "backup@example.com"},
				},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts, err := smtptest.NewServer()
				if err != nil {
					return nil, err
				}
				ctxt := context.WithValue(nil, "server", ts)

				c.SMTP.Enabled = true
				c.SMTP.Host = ts.Host
				c.SMTP.Port = ts.Port
				c.SMTP.From = "test@example.com"
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*smtptest.Server)
				ts.Close()

				errors := ts.Errors()
				if len(errors) != 0 {
					return fmt.Errorf("multiple errors %d: %v", len(errors), errors)
				}

				expMail := []*smtptest.Message{{
					Header: mail.Header{
						"Mime-Version":              []string{"1.0"},
						"Content-Type":              []string{"text/html; charset=UTF-8"},
						"Content-Transfer-Encoding": []string{"quoted-printable"},
						"To":                        []string{"oncall@example.com, backup@example.com"},
						"From":                      []string{"test@example.com"},
						"Subject":                   []string{"message"},
					},
					Body: "details\n",
				}}
				msgs := ts.SentMessages()
				if got, exp := len(msgs), len(expMail); got != exp {
					return fmt.Errorf("unexpected number of messages sent: got %d exp %d", got, exp)
				}
				for i, exp := range expMail {
					got := msgs[i]
					if err := exp.Compare(got); err != nil {
						return fmt.Errorf("unexpected message %d: %v", i, err)
					}
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "snmptrap",
				Options: map[string]interface{}{
					"trap-oid": "1.1.2",
					"data-list": []map[string]string{
						{
							"oid":   "1.1.2.1",
							"type":  "s",
							"value": "{{.Message}}",
						},
						{
							"oid":   "1.1.2.2",
							"type":  "s",
							"value": "{{.Level}}",
						},
					},
				},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts, err := snmptraptest.NewServer()
				if err != nil {
					return nil, err
				}
				ctxt := context.WithValue(nil, "server", ts)

				c.SNMPTrap.Enabled = true
				c.SNMPTrap.Addr = ts.Addr
				c.SNMPTrap.Community = ts.Community
				c.SNMPTrap.Retries = 3
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*snmptraptest.Server)
				ts.Close()
				got := ts.Traps()
				exp := []snmptraptest.Trap{{
					Pdu: snmptraptest.Pdu{
						Type:        snmpgo.SNMPTrapV2,
						ErrorStatus: snmpgo.NoError,
						VarBinds: snmptraptest.VarBinds{
							{
								Oid:   "1.3.6.1.2.1.1.3.0",
								Value: "1000",
								Type:  "TimeTicks",
							},
							{
								Oid:   "1.3.6.1.6.3.1.1.4.1.0",
								Value: "1.1.2",
								Type:  "Oid",
							},
							{
								Oid:   "1.1.2.1",
								Value: "message",
								Type:  "OctetString",
							},
							{
								Oid:   "1.1.2.2",
								Value: "CRITICAL",
								Type:  "OctetString",
							},
						},
					},
				}}
				if !reflect.DeepEqual(exp, got) {
					return fmt.Errorf("unexpected snmptrap request:\nexp\n%+v\ngot\n%+v\n", exp, got)
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "talk",
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts := talktest.NewServer()
				ctxt := context.WithValue(nil, "server", ts)

				c.Talk.Enabled = true
				c.Talk.URL = ts.URL
				c.Talk.AuthorName = "Kapacitor"
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*talktest.Server)
				ts.Close()
				got := ts.Requests()
				exp := []talktest.Request{{
					URL: "/",
					PostData: talktest.PostData{
						AuthorName: "Kapacitor",
						Text:       "message",
						Title:      "id",
					},
				}}
				if !reflect.DeepEqual(exp, got) {
					return fmt.Errorf("unexpected talk request:\nexp\n%+v\ngot\n%+v\n", exp, got)
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "tcp",
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts, err := alerttest.NewTCPServer()
				if err != nil {
					return nil, err
				}

				ha.Options = map[string]interface{}{"address": ts.Addr}

				ctxt := context.WithValue(nil, "server", ts)
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*alerttest.TCPServer)
				ts.Close()
				exp := []alert.Data{alertData}
				got := ts.Data()
				if !reflect.DeepEqual(exp, got) {
					return fmt.Errorf("unexpected tcp request:\nexp\n%+v\ngot\n%+v\n", exp, got)
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "telegram",
				Options: map[string]interface{}{
					"chat-id":                  "chat id",
					"disable-web-page-preview": true,
				},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts := telegramtest.NewServer()
				ctxt := context.WithValue(nil, "server", ts)

				c.Telegram.Enabled = true
				c.Telegram.URL = ts.URL + "/bot"
				c.Telegram.Token = "TOKEN:AUTH"
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*telegramtest.Server)
				ts.Close()
				got := ts.Requests()
				exp := []telegramtest.Request{{
					URL: "/botTOKEN:AUTH/sendMessage",
					PostData: telegramtest.PostData{
						ChatId:                "chat id",
						Text:                  "message",
						ParseMode:             "",
						DisableWebPagePreview: true,
						DisableNotification:   false,
					},
				}}
				if !reflect.DeepEqual(exp, got) {
					return fmt.Errorf("unexpected telegram request:\nexp\n%+v\ngot\n%+v\n", exp, got)
				}
				return nil
			},
		},
		{
			handler: client.TopicHandler{
				Kind: "victorops",
				Options: map[string]interface{}{
					"routing-key": "key",
				},
			},
			setup: func(c *server.Config, ha *client.TopicHandler) (context.Context, error) {
				ts := victoropstest.NewServer()
				ctxt := context.WithValue(nil, "server", ts)

				c.VictorOps.Enabled = true
				c.VictorOps.URL = ts.URL
				c.VictorOps.APIKey = "api_key"
				return ctxt, nil
			},
			result: func(ctxt context.Context) error {
				ts := ctxt.Value("server").(*victoropstest.Server)
				ts.Close()
				got := ts.Requests()
				exp := []victoropstest.Request{{
					URL: "/api_key/key",
					PostData: victoropstest.PostData{
						MessageType:    "CRITICAL",
						EntityID:       "id",
						StateMessage:   "message",
						Timestamp:      0,
						MonitoringTool: "kapacitor",
						Data:           resultJSON,
					},
				}}
				if !reflect.DeepEqual(exp, got) {
					return fmt.Errorf("unexpected victorops request:\nexp\n%+v\ngot\n%+v\n", exp, got)
				}
				return nil
			},
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%s-%d", tc.handler.Kind, i), func(t *testing.T) {
			kind := tc.handler.Kind
			// Create default config
			c := NewConfig()
			var ctxt context.Context
			if tc.setup != nil {
				var err error
				ctxt, err = tc.setup(c, &tc.handler)
				if err != nil {
					t.Fatal(err)
				}
			}
			s := OpenServer(c)
			cli := Client(s)
			closed := false
			defer func() {
				if !closed {
					s.Close()
				}
			}()
			ctxt = context.WithValue(ctxt, "kapacitorURL", s.URL())

			if _, err := cli.CreateTopicHandler(cli.TopicHandlersLink("test"), client.TopicHandlerOptions{
				ID:      "testAlertHandlers",
				Kind:    tc.handler.Kind,
				Options: tc.handler.Options,
			}); err != nil {
				t.Fatalf("%s: %v", kind, err)
			}

			tick := `
stream
	|from()
		.measurement('alert')
	|alert()
		.topic('test')
		.id('id')
		.message('message')
		.details('details')
		.crit(lambda: TRUE)
`

			if _, err := cli.CreateTask(client.CreateTaskOptions{
				ID:   "testAlertHandlers",
				Type: client.StreamTask,
				DBRPs: []client.DBRP{{
					Database:        "mydb",
					RetentionPolicy: "myrp",
				}},
				TICKscript: tick,
				Status:     client.Enabled,
			}); err != nil {
				t.Fatalf("%s: %v", kind, err)
			}

			point := "alert value=1 0000000000"
			v := url.Values{}
			v.Add("precision", "s")
			s.MustWrite("mydb", "myrp", point, v)

			// Close the entire server to ensure all data is processed
			s.Close()
			closed = true

			if err := tc.result(ctxt); err != nil {
				t.Errorf("%s: %v", kind, err)
			}
		})
	}
}

func TestServer_Alert_Duration(t *testing.T) {
	// Setup test TCP server
	ts, err := alerttest.NewTCPServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()

	// Create default config
	c := NewConfig()
	s := OpenServer(c)
	cli := Client(s)
	defer s.Close()

	tick := `
stream
	|from()
		.measurement('alert')
	|alert()
		.id('id')
		.message('message')
		.details('details')
		.crit(lambda: "value" > 1.0)
		.tcp('` + ts.Addr + `')
`

	if _, err := cli.CreateTask(client.CreateTaskOptions{
		ID:   "testAlertHandlers",
		Type: client.StreamTask,
		DBRPs: []client.DBRP{{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		}},
		TICKscript: tick,
		Status:     client.Enabled,
	}); err != nil {
		t.Fatal(err)
	}

	// Write point
	point := "alert value=2 0000000000"
	v := url.Values{}
	v.Add("precision", "s")
	s.MustWrite("mydb", "myrp", point, v)

	// Restart the server
	s.Restart()

	topic := "main:testAlertHandlers:alert2"
	l := cli.TopicEventsLink(topic)
	expTopicEvents := client.TopicEvents{
		Link:  l,
		Topic: topic,
		Events: []client.TopicEvent{{
			Link: client.Link{Relation: client.Self, Href: fmt.Sprintf("/kapacitor/v1/alerts/topics/%s/events/id", topic)},
			ID:   "id",
			State: client.EventState{
				Message:  "message",
				Details:  "details",
				Time:     time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				Duration: 0,
				Level:    "CRITICAL",
			},
		}},
	}

	te, err := cli.ListTopicEvents(l, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(te, expTopicEvents) {
		t.Errorf("unexpected topic events for anonymous topic:\ngot\n%+v\nexp\n%+v\n", te, expTopicEvents)
	}
	event, err := cli.TopicEvent(expTopicEvents.Events[0].Link)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(event, expTopicEvents.Events[0]) {
		t.Errorf("unexpected topic event for anonymous topic:\ngot\n%+v\nexp\n%+v\n", event, expTopicEvents.Events[0])
	}

	// Write point
	point = "alert value=3 0000000001"
	v = url.Values{}
	v.Add("precision", "s")
	s.MustWrite("mydb", "myrp", point, v)

	// Restart the server
	s.Restart()

	expTopicEvents = client.TopicEvents{
		Link:  l,
		Topic: topic,
		Events: []client.TopicEvent{{
			Link: client.Link{Relation: client.Self, Href: fmt.Sprintf("/kapacitor/v1/alerts/topics/%s/events/id", topic)},
			ID:   "id",
			State: client.EventState{
				Message:  "message",
				Details:  "details",
				Time:     time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC),
				Duration: client.Duration(time.Second),
				Level:    "CRITICAL",
			},
		}},
	}

	te, err = cli.ListTopicEvents(l, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(te, expTopicEvents) {
		t.Errorf("unexpected topic events for anonymous topic after second point:\ngot\n%+v\nexp\n%+v\n", te, expTopicEvents)
	}
}

func TestServer_Alert_Aggregate(t *testing.T) {
	// Setup test TCP server
	ts, err := alerttest.NewTCPServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()

	// Create default config
	c := NewConfig()
	s := OpenServer(c)
	cli := Client(s)
	defer s.Close()

	aggTopic := "agg"

	// Create task for alert
	tick := `
stream
	|from()
		.measurement('alert')
	|alert()
		.id('id')
		.message('message')
		.details('details')
		.crit(lambda: "value" > 1.0)
		.topic('` + aggTopic + `')
`

	if _, err := cli.CreateTask(client.CreateTaskOptions{
		ID:   "agg_task",
		Type: client.StreamTask,
		DBRPs: []client.DBRP{{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		}},
		TICKscript: tick,
		Status:     client.Enabled,
	}); err != nil {
		t.Fatal(err)
	}

	// Create tpc handler on tcp topic
	tcpTopic := "tcp"
	if _, err := cli.CreateTopicHandler(cli.TopicHandlersLink(tcpTopic), client.TopicHandlerOptions{
		ID:   "tcp_handler",
		Kind: "tcp",
		Options: map[string]interface{}{
			"address": ts.Addr,
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Create aggregate handler on agg topic
	if _, err := cli.CreateTopicHandler(cli.TopicHandlersLink(aggTopic), client.TopicHandlerOptions{
		ID:   "aggregate_handler",
		Kind: "aggregate",
		Options: map[string]interface{}{
			"id":       "id-agg",
			"interval": 100 * time.Millisecond,
			"topic":    "tcp",
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Write points
	point := `alert value=3 0000000000000
alert value=4 0000000000001
alert value=2 0000000000002
`
	v := url.Values{}
	v.Add("precision", "ms")
	s.MustWrite("mydb", "myrp", point, v)

	time.Sleep(110 * time.Millisecond)

	// Check TCP handler got event
	alertData := alert.Data{
		ID:          "id-agg",
		Message:     "Received 3 events in the last 100ms.",
		Details:     "message\nmessage\nmessage",
		Time:        time.Date(1970, 1, 1, 0, 0, 0, 2000000, time.UTC),
		Level:       alert.Critical,
		Duration:    2 * time.Millisecond,
		Recoverable: false,
		Data: models.Result{
			Series: models.Rows{
				{
					Name:    "alert",
					Columns: []string{"time", "value"},
					Values: [][]interface{}{[]interface{}{
						time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					}},
				},
				{
					Name:    "alert",
					Columns: []string{"time", "value"},
					Values: [][]interface{}{[]interface{}{
						time.Date(1970, 1, 1, 0, 0, 0, 1000000, time.UTC),
						4.0,
					}},
				},
				{
					Name:    "alert",
					Columns: []string{"time", "value"},
					Values: [][]interface{}{[]interface{}{
						time.Date(1970, 1, 1, 0, 0, 0, 2000000, time.UTC),
						2.0,
					}},
				},
			},
		},
	}
	ts.Close()
	exp := []alert.Data{alertData}
	got := ts.Data()
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("unexpected tcp request:\nexp\n%+v\ngot\n%+v\n", exp, got)
	}

	// Check event on topic
	l := cli.TopicEventsLink(tcpTopic)
	expTopicEvents := client.TopicEvents{
		Link:  l,
		Topic: tcpTopic,
		Events: []client.TopicEvent{{
			Link: client.Link{Relation: client.Self, Href: fmt.Sprintf("/kapacitor/v1/alerts/topics/%s/events/id-agg", tcpTopic)},
			ID:   "id-agg",
			State: client.EventState{
				Message:  "Received 3 events in the last 100ms.",
				Details:  "message\nmessage\nmessage",
				Time:     time.Date(1970, 1, 1, 0, 0, 0, 2000000, time.UTC),
				Duration: client.Duration(2 * time.Millisecond),
				Level:    "CRITICAL",
			},
		}},
	}

	te, err := cli.ListTopicEvents(l, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(te, expTopicEvents) {
		t.Errorf("unexpected topic events for aggregate topic:\ngot\n%+v\nexp\n%+v\n", te, expTopicEvents)
	}
}

func TestServer_Alert_Publish(t *testing.T) {
	// Setup test TCP server
	ts, err := alerttest.NewTCPServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()

	// Create default config
	c := NewConfig()
	s := OpenServer(c)
	cli := Client(s)
	defer s.Close()

	publishTopic := "publish"

	// Create task for alert
	tick := `
stream
	|from()
		.measurement('alert')
	|alert()
		.id('id')
		.message('message')
		.details('details')
		.crit(lambda: "value" > 1.0)
		.topic('` + publishTopic + `')
`

	if _, err := cli.CreateTask(client.CreateTaskOptions{
		ID:   "publish_task",
		Type: client.StreamTask,
		DBRPs: []client.DBRP{{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		}},
		TICKscript: tick,
		Status:     client.Enabled,
	}); err != nil {
		t.Fatal(err)
	}

	// Create tpc handler on tcp topic
	tcpTopic := "tcp"
	if _, err := cli.CreateTopicHandler(cli.TopicHandlersLink(tcpTopic), client.TopicHandlerOptions{
		ID:   "tcp_handler",
		Kind: "tcp",
		Options: map[string]interface{}{
			"address": ts.Addr,
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Create publish handler on publish topic
	if _, err := cli.CreateTopicHandler(cli.TopicHandlersLink(publishTopic), client.TopicHandlerOptions{
		ID:   "publish_handler",
		Kind: "publish",
		Options: map[string]interface{}{
			// Publish to tcpTopic
			"topics": []string{tcpTopic},
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Write points
	point := `alert value=2 0000000000`
	v := url.Values{}
	v.Add("precision", "s")
	s.MustWrite("mydb", "myrp", point, v)

	s.Restart()

	// Check TCP handler got event
	alertData := alert.Data{
		ID:          "id",
		Message:     "message",
		Details:     "details",
		Time:        time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		Level:       alert.Critical,
		Recoverable: true,
		Data: models.Result{
			Series: models.Rows{
				{
					Name:    "alert",
					Columns: []string{"time", "value"},
					Values: [][]interface{}{[]interface{}{
						time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
						2.0,
					}},
				},
			},
		},
	}
	ts.Close()
	exp := []alert.Data{alertData}
	got := ts.Data()
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("unexpected tcp request:\nexp\n%+v\ngot\n%+v\n", exp, got)
	}

	// Check event on topic
	l := cli.TopicEventsLink(tcpTopic)
	expTopicEvents := client.TopicEvents{
		Link:  l,
		Topic: tcpTopic,
		Events: []client.TopicEvent{{
			Link: client.Link{Relation: client.Self, Href: fmt.Sprintf("/kapacitor/v1/alerts/topics/%s/events/id", tcpTopic)},
			ID:   "id",
			State: client.EventState{
				Message:  "message",
				Details:  "details",
				Time:     time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				Duration: 0,
				Level:    "CRITICAL",
			},
		}},
	}

	te, err := cli.ListTopicEvents(l, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(te, expTopicEvents) {
		t.Errorf("unexpected topic events for publish topic:\ngot\n%+v\nexp\n%+v\n", te, expTopicEvents)
	}
}

func TestServer_Alert_Match(t *testing.T) {
	// Setup test TCP server
	ts, err := alerttest.NewTCPServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()

	// Create default config
	c := NewConfig()
	s := OpenServer(c)
	cli := Client(s)
	defer s.Close()

	topic := "test"

	// Create task for alert
	tick := `
stream
	|from()
		.measurement('alert')
	|alert()
		.id('id')
		.message('message')
		.details('details')
		.crit(lambda: "value" > 1.0)
		.topic('` + topic + `')
`

	if _, err := cli.CreateTask(client.CreateTaskOptions{
		ID:   "alert_task",
		Type: client.StreamTask,
		DBRPs: []client.DBRP{{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		}},
		TICKscript: tick,
		Status:     client.Enabled,
	}); err != nil {
		t.Fatal(err)
	}

	// Create tpc handler with match condition
	if _, err := cli.CreateTopicHandler(cli.TopicHandlersLink(topic), client.TopicHandlerOptions{
		ID:   "tcp_handler",
		Kind: "tcp",
		Options: map[string]interface{}{
			"address": ts.Addr,
		},
		Match: `"host" == 'serverA' AND level() == CRITICAL`,
	}); err != nil {
		t.Fatal(err)
	}

	// Write points
	point := `alert,host=serverA value=0 0000000000
alert,host=serverB value=2 0000000001
alert,host=serverB value=0 0000000002
alert,host=serverA value=2 0000000003
alert,host=serverB value=0 0000000004
`
	v := url.Values{}
	v.Add("precision", "s")
	s.MustWrite("mydb", "myrp", point, v)

	s.Restart()

	alertData := alert.Data{
		ID:          "id",
		Message:     "message",
		Details:     "details",
		Time:        time.Date(1970, 1, 1, 0, 0, 3, 0, time.UTC),
		Level:       alert.Critical,
		Recoverable: true,
		Data: models.Result{
			Series: models.Rows{
				{
					Name:    "alert",
					Tags:    map[string]string{"host": "serverA"},
					Columns: []string{"time", "value"},
					Values: [][]interface{}{[]interface{}{
						time.Date(1970, 1, 1, 0, 0, 3, 0, time.UTC),
						2.0,
					}},
				},
			},
		},
	}
	ts.Close()
	exp := []alert.Data{alertData}
	got := ts.Data()
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("unexpected tcp request:\nexp\n%+v\ngot\n%+v\n", exp, got)
	}

	// Topic should have must recent event
	l := cli.TopicEventsLink(topic)
	expTopicEvents := client.TopicEvents{
		Link:  l,
		Topic: topic,
		Events: []client.TopicEvent{{
			Link: client.Link{Relation: client.Self, Href: fmt.Sprintf("/kapacitor/v1/alerts/topics/%s/events/id", topic)},
			ID:   "id",
			State: client.EventState{
				Message:  "message",
				Details:  "details",
				Time:     time.Date(1970, 1, 1, 0, 0, 4, 0, time.UTC),
				Duration: client.Duration(time.Second),
				Level:    "OK",
			},
		}},
	}

	te, err := cli.ListTopicEvents(l, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(te, expTopicEvents) {
		t.Errorf("unexpected topic events for publish topic:\ngot\n%+v\nexp\n%+v\n", te, expTopicEvents)
	}
}

func TestServer_AlertAnonTopic(t *testing.T) {
	// Setup test TCP server
	ts, err := alerttest.NewTCPServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()

	// Create default config
	c := NewConfig()
	s := OpenServer(c)
	cli := Client(s)
	defer s.Close()

	tick := `
stream
	|from()
		.measurement('alert')
	|alert()
		.id('id')
		.message('message')
		.details('details')
		.warn(lambda: "value" <= 1.0)
		.crit(lambda: "value" > 1.0)
		.tcp('` + ts.Addr + `')
`

	task, err := cli.CreateTask(client.CreateTaskOptions{
		ID:   "testAlertHandlers",
		Type: client.StreamTask,
		DBRPs: []client.DBRP{{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		}},
		TICKscript: tick,
		Status:     client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Write warning point
	point := "alert value=1 0000000000"
	v := url.Values{}
	v.Add("precision", "s")
	s.MustWrite("mydb", "myrp", point, v)

	// Restart the server
	s.Restart()

	topic := "main:testAlertHandlers:alert2"
	l := cli.TopicEventsLink(topic)
	expTopicEvents := client.TopicEvents{
		Link:  l,
		Topic: topic,
		Events: []client.TopicEvent{{
			Link: client.Link{Relation: client.Self, Href: fmt.Sprintf("/kapacitor/v1/alerts/topics/%s/events/id", topic)},
			ID:   "id",
			State: client.EventState{
				Message:  "message",
				Details:  "details",
				Time:     time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				Duration: 0,
				Level:    "WARNING",
			},
		}},
	}

	te, err := cli.ListTopicEvents(l, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(te, expTopicEvents) {
		t.Errorf("unexpected topic events for anonymous topic:\ngot\n%+v\nexp\n%+v\n", te, expTopicEvents)
	}
	event, err := cli.TopicEvent(expTopicEvents.Events[0].Link)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(event, expTopicEvents.Events[0]) {
		t.Errorf("unexpected topic event for anonymous topic:\ngot\n%+v\nexp\n%+v\n", event, expTopicEvents.Events[0])
	}

	// Disable task
	task, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Disabled,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := cli.ListTopicEvents(l, nil); err == nil {
		t.Fatal("expected error listing anonymous topic for disabled task")
	} else if got, exp := err.Error(), fmt.Sprintf("failed to get topic events: unknown topic %q", topic); got != exp {
		t.Errorf("unexpected error message for nonexistent anonymous topic: got %q exp %q", got, exp)
	}

	// Enable task
	task, err = cli.UpdateTask(task.Link, client.UpdateTaskOptions{
		Status: client.Enabled,
	})
	if err != nil {
		t.Fatal(err)
	}
	te, err = cli.ListTopicEvents(l, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(te, expTopicEvents) {
		t.Errorf("unexpected topic events for anonymous topic after re-enable:\ngot\n%+v\nexp\n%+v\n", te, expTopicEvents)
	}

	// Restart the server, again and ensure that the anonymous topic state is restored
	s.Restart()
	te, err = cli.ListTopicEvents(l, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(te, expTopicEvents) {
		t.Errorf("unexpected topic events for anonymous topic after re-enable and restart:\ngot\n%+v\nexp\n%+v\n", te, expTopicEvents)
	}

	// Delete task
	if err := cli.DeleteTask(task.Link); err != nil {
		t.Fatal(err)
	}

	if _, err := cli.ListTopicEvents(l, nil); err == nil {
		t.Fatal("expected error listing anonymous topic for deleted task")
	} else if got, exp := err.Error(), fmt.Sprintf("failed to get topic events: unknown topic %q", topic); got != exp {
		t.Errorf("unexpected error message for nonexistent anonymous topic: got %q exp %q", got, exp)
	}
}

func TestServer_AlertTopic_PersistedState(t *testing.T) {
	// Setup test TCP server
	ts, err := alerttest.NewTCPServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()

	tmpDir := MustTempDir()
	defer os.RemoveAll(tmpDir)
	tmpPath := filepath.Join(tmpDir, "alert.log")

	// Create default config
	c := NewConfig()
	s := OpenServer(c)
	cli := Client(s)
	defer s.Close()

	if _, err := cli.CreateTopicHandler(cli.TopicHandlersLink("test"), client.TopicHandlerOptions{
		ID:      "testAlertHandler",
		Kind:    "tcp",
		Options: map[string]interface{}{"address": ts.Addr},
	}); err != nil {
		t.Fatal(err)
	}

	tick := `
stream
	|from()
		.measurement('alert')
	|alert()
		.topic('test')
		.id('id')
		.message('message')
		.details('details')
		.warn(lambda: TRUE)
		.log('` + tmpPath + `')
`

	if _, err := cli.CreateTask(client.CreateTaskOptions{
		ID:   "testAlertHandlers",
		Type: client.StreamTask,
		DBRPs: []client.DBRP{{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		}},
		TICKscript: tick,
		Status:     client.Enabled,
	}); err != nil {
		t.Fatal(err)
	}

	point := "alert value=1 0000000000"
	v := url.Values{}
	v.Add("precision", "s")
	s.MustWrite("mydb", "myrp", point, v)

	// Restart the server
	s.Restart()

	topics := []string{
		"test",
		"main:testAlertHandlers:alert2",
	}
	for _, topic := range topics {
		l := cli.TopicEventsLink(topic)
		expTopicEvents := client.TopicEvents{
			Link:  l,
			Topic: topic,
			Events: []client.TopicEvent{{
				Link: client.Link{Relation: client.Self, Href: fmt.Sprintf("/kapacitor/v1/alerts/topics/%s/events/id", topic)},
				ID:   "id",
				State: client.EventState{
					Message:  "message",
					Details:  "details",
					Time:     time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
					Duration: 0,
					Level:    "WARNING",
				},
			}},
		}

		te, err := cli.ListTopicEvents(l, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(te, expTopicEvents) {
			t.Errorf("unexpected topic events for topic %q:\ngot\n%+v\nexp\n%+v\n", topic, te, expTopicEvents)
		}
		event, err := cli.TopicEvent(expTopicEvents.Events[0].Link)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(event, expTopicEvents.Events[0]) {
			t.Errorf("unexpected topic event for topic %q:\ngot\n%+v\nexp\n%+v\n", topic, event, expTopicEvents.Events[0])
		}

		te, err = cli.ListTopicEvents(l, &client.ListTopicEventsOptions{
			MinLevel: "CRITICAL",
		})
		if err != nil {
			t.Fatal(err)
		}
		expTopicEvents.Events = expTopicEvents.Events[0:0]
		if !reflect.DeepEqual(te, expTopicEvents) {
			t.Errorf("unexpected topic events with minLevel for topic %q:\ngot\n%+v\nexp\n%+v\n", topic, te, expTopicEvents)
		}

		l = cli.TopicLink(topic)
		if err := cli.DeleteTopic(l); err != nil {
			t.Fatal(err)
		}
		te, err = cli.ListTopicEvents(l, nil)
		if err == nil {
			t.Fatalf("expected error for deleted topic %q", topic)
		}
	}
}
func TestServer_Alert_Inhibition(t *testing.T) {
	// Test Overview
	// Create several alerts:
	//  * cpu - alert on host cpu usage by region,host,cpu
	//  * mem - alert on host mem usage by region,host
	//  * host - alert on host up/down by region,host
	//  * region - alert on region up/down by region
	//
	// The host alert will inhibit the cpu and mem alerts by host
	// The region alert will inhibit the cpu mem and host alerts by region
	//

	// Create default config
	c := NewConfig()
	s := OpenServer(c)
	cli := Client(s)
	closed := false
	defer func() {
		if !closed {
			s.Close()
		}
	}()

	// Setup test TCP server
	ts, err := alerttest.NewTCPServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()

	if _, err := cli.CreateTopicHandler(cli.TopicHandlersLink("inhibition"), client.TopicHandlerOptions{
		ID:      "tcpHandler",
		Kind:    "tcp",
		Options: map[string]interface{}{"address": ts.Addr},
	}); err != nil {
		t.Fatal(err)
	}

	memAlert := `
stream
	|from()
		.measurement('mem')
		.groupBy(*)
	|alert()
		.category('system')
		.topic('inhibition')
		.message('mem')
		.details('')
		.crit(lambda: "v")
`
	cpuAlert := `
stream
	|from()
		.measurement('cpu')
		.groupBy(*)
	|alert()
		.category('system')
		.topic('inhibition')
		.message('cpu')
		.details('')
		.crit(lambda: "v")
`
	hostAlert := `
stream
	|from()
		.measurement('host')
		.groupBy(*)
	|alert()
		.category('host_alert')
		.topic('inhibition')
		.message('host')
		.details('')
		.crit(lambda: "v")
		.inhibit('system', 'region', 'host')
`
	regionAlert := `
stream
	|from()
		.measurement('region')
		.groupBy(*)
	|alert()
		.category('region_alert')
		.topic('inhibition')
		.message('region')
		.details('')
		.crit(lambda: "v")
		.inhibit('host_alert', 'region')
		.inhibit('system', 'region')
`

	tasks := map[string]string{
		"cpu":    cpuAlert,
		"mem":    memAlert,
		"host":   hostAlert,
		"region": regionAlert,
	}
	for id, tick := range tasks {
		if _, err := cli.CreateTask(client.CreateTaskOptions{
			ID:   id,
			Type: client.StreamTask,
			DBRPs: []client.DBRP{{
				Database:        "mydb",
				RetentionPolicy: "myrp",
			}},
			TICKscript: tick,
			Status:     client.Enabled,
		}); err != nil {
			t.Fatal(err)
		}
	}

	batches := []string{
		//#0 Send initial batch with all alerts in the green state
		`cpu,region=west,host=A,cpu=0 v=false 0
cpu,region=west,host=A,cpu=1 v=false 0
cpu,region=west,host=B,cpu=0 v=false 0
cpu,region=west,host=B,cpu=1 v=false 0
cpu,region=east,host=A,cpu=0 v=false 0
cpu,region=east,host=A,cpu=1 v=false 0
cpu,region=east,host=B,cpu=0 v=false 0
cpu,region=east,host=B,cpu=1 v=false 0
mem,region=west,host=A v=false 0
mem,region=west,host=B v=false 0
mem,region=east,host=A v=false 0
mem,region=east,host=B v=false 0
host,region=west,host=A v=false 0
host,region=west,host=B v=false 0
host,region=east,host=A v=false 0
host,region=east,host=B v=false 0
region,region=west v=false 0
region,region=east v=false 0
`,
		//#1 Send batch where some mem and cpu alerts fire
		`cpu,region=west,host=B,cpu=0 v=true 1
cpu,region=east,host=A,cpu=1 v=true 1
mem,region=west,host=B v=true 1
mem,region=east,host=A v=true 1
`,
		//#2 Send batch where some host alerts fire
		`host,region=west,host=B v=true 2
host,region=east,host=B v=true 2
`,
		//#3 Send batch where some mem and cpu alerts fire
		`cpu,region=west,host=B,cpu=0 v=true 3
cpu,region=east,host=A,cpu=1 v=true 3
mem,region=west,host=B v=true 3
mem,region=east,host=A v=true 3
`,
		//#4 Send batch were hosts alerts recover
		`host,region=west,host=B v=false 4
host,region=east,host=B v=false 4
`,
		//#5 Send batch where some mem and cpu alerts fire
		`cpu,region=west,host=B,cpu=0 v=true 5
cpu,region=east,host=A,cpu=1 v=true 5
mem,region=west,host=B v=true 5
mem,region=east,host=A v=true 5
`,
		//#6 Send batch where region alert fires
		`region,region=east v=true 6`,

		//#7 Send batch where some mem, cpu and host alerts fire
		`cpu,region=west,host=B,cpu=0 v=true 7
cpu,region=east,host=A,cpu=1 v=true 7
mem,region=west,host=B v=true 7
mem,region=east,host=A v=true 7
host,region=west,host=A v=true 7
host,region=east,host=B v=true 7
`,
		//#8 Send batch where region alert recovers
		`region,region=east v=false 8`,

		//#9 Send batch where some mem, cpu and host alerts fire
		`cpu,region=west,host=B,cpu=0 v=true 9
cpu,region=east,host=A,cpu=1 v=true 9
mem,region=west,host=B v=true 9
mem,region=east,host=A v=true 9
host,region=west,host=A v=true 9
host,region=east,host=B v=true 9
`,
	}

	v := url.Values{}
	v.Add("precision", "s")
	for _, p := range batches {
		s.MustWrite("mydb", "myrp", p, v)
		time.Sleep(50 * time.Millisecond)
	}

	// Close the entire server to ensure all data is processed
	s.Close()
	closed = true

	want := []alert.Data{
		// #1

		{
			ID:            "cpu:cpu=0,host=B,region=west",
			Message:       "cpu",
			Time:          time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.OK,
			Duration:      0,
			Recoverable:   true,
		},
		{
			ID:            "cpu:cpu=1,host=A,region=east",
			Message:       "cpu",
			Time:          time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.OK,
			Duration:      0,
			Recoverable:   true,
		},
		{
			ID:            "mem:host=A,region=east",
			Message:       "mem",
			Time:          time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.OK,
			Duration:      0,
			Recoverable:   true,
		},
		{
			ID:            "mem:host=B,region=west",
			Message:       "mem",
			Time:          time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.OK,
			Duration:      0,
			Recoverable:   true,
		},

		// #2

		{
			ID:            "host:host=B,region=east",
			Message:       "host",
			Time:          time.Date(1970, 1, 1, 0, 0, 2, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.OK,
			Duration:      0,
			Recoverable:   true,
		},
		{
			ID:            "host:host=B,region=west",
			Message:       "host",
			Time:          time.Date(1970, 1, 1, 0, 0, 2, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.OK,
			Duration:      0,
			Recoverable:   true,
		},

		// #3

		{
			ID:            "cpu:cpu=1,host=A,region=east",
			Message:       "cpu",
			Time:          time.Date(1970, 1, 1, 0, 0, 3, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      2 * time.Second,
			Recoverable:   true,
		},
		{
			ID:            "mem:host=A,region=east",
			Message:       "mem",
			Time:          time.Date(1970, 1, 1, 0, 0, 3, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      2 * time.Second,
			Recoverable:   true,
		},

		// #4

		{
			ID:            "host:host=B,region=east",
			Message:       "host",
			Time:          time.Date(1970, 1, 1, 0, 0, 4, 0, time.UTC),
			Level:         alert.OK,
			PreviousLevel: alert.Critical,
			Duration:      2 * time.Second,
			Recoverable:   true,
		},
		{
			ID:            "host:host=B,region=west",
			Message:       "host",
			Time:          time.Date(1970, 1, 1, 0, 0, 4, 0, time.UTC),
			Level:         alert.OK,
			PreviousLevel: alert.Critical,
			Duration:      2 * time.Second,
			Recoverable:   true,
		},

		// #5

		{
			ID:            "cpu:cpu=0,host=B,region=west",
			Message:       "cpu",
			Time:          time.Date(1970, 1, 1, 0, 0, 5, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      4 * time.Second,
			Recoverable:   true,
		},
		{
			ID:            "cpu:cpu=1,host=A,region=east",
			Message:       "cpu",
			Time:          time.Date(1970, 1, 1, 0, 0, 5, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      4 * time.Second,
			Recoverable:   true,
		},
		{
			ID:            "mem:host=A,region=east",
			Message:       "mem",
			Time:          time.Date(1970, 1, 1, 0, 0, 5, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      4 * time.Second,
			Recoverable:   true,
		},
		{
			ID:            "mem:host=B,region=west",
			Message:       "mem",
			Time:          time.Date(1970, 1, 1, 0, 0, 5, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      4 * time.Second,
			Recoverable:   true,
		},

		// #6

		{
			ID:            "region:region=east",
			Message:       "region",
			Time:          time.Date(1970, 1, 1, 0, 0, 6, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.OK,
			Duration:      0,
			Recoverable:   true,
		},

		// #7
		{
			ID:            "cpu:cpu=0,host=B,region=west",
			Message:       "cpu",
			Time:          time.Date(1970, 1, 1, 0, 0, 7, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      6 * time.Second,
			Recoverable:   true,
		},
		{
			ID:            "host:host=A,region=west",
			Message:       "host",
			Time:          time.Date(1970, 1, 1, 0, 0, 7, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.OK,
			Duration:      0,
			Recoverable:   true,
		},
		{
			ID:            "mem:host=B,region=west",
			Message:       "mem",
			Time:          time.Date(1970, 1, 1, 0, 0, 7, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      6 * time.Second,
			Recoverable:   true,
		},

		// #8

		{
			ID:            "region:region=east",
			Message:       "region",
			Time:          time.Date(1970, 1, 1, 0, 0, 8, 0, time.UTC),
			Level:         alert.OK,
			PreviousLevel: alert.Critical,
			Duration:      2 * time.Second,
			Recoverable:   true,
		},

		// #9

		{
			ID:            "cpu:cpu=0,host=B,region=west",
			Message:       "cpu",
			Time:          time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      8 * time.Second,
			Recoverable:   true,
		},
		{
			ID:            "cpu:cpu=1,host=A,region=east",
			Message:       "cpu",
			Time:          time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      8 * time.Second,
			Recoverable:   true,
		},
		{
			ID:            "host:host=A,region=west",
			Message:       "host",
			Time:          time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      2 * time.Second,
			Recoverable:   true,
		},
		{
			ID:            "host:host=B,region=east",
			Message:       "host",
			Time:          time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.OK,
			Duration:      2 * time.Second,
			Recoverable:   true,
		},
		{
			ID:            "mem:host=A,region=east",
			Message:       "mem",
			Time:          time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      8 * time.Second,
			Recoverable:   true,
		},
		{
			ID:            "mem:host=B,region=west",
			Message:       "mem",
			Time:          time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
			Level:         alert.Critical,
			PreviousLevel: alert.Critical,
			Duration:      8 * time.Second,
			Recoverable:   true,
		},
	}
	ts.Close()
	got := ts.Data()
	// Remove the .Data result from the alerts
	for i := range got {
		got[i].Data = models.Result{}
	}
	// Sort results since order doesn't matter
	//sort.Slice(want, func(i, j int) bool {
	//	if want[i].Time.Equal(want[j].Time) {
	//		return want[i].ID < want[j].ID
	//	}
	//	return want[i].Time.Before(want[j].Time)
	//})
	sort.Slice(got, func(i, j int) bool {
		if got[i].Time.Equal(got[j].Time) {
			return got[i].ID < got[j].ID
		}
		return got[i].Time.Before(got[j].Time)
	})
	t.Logf("want: %d got: %d", len(want), len(got))
	if !cmp.Equal(got, want) {
		t.Errorf("unexpected alert during inhibited run -want/+got\n%s", cmp.Diff(want, got))
	}
	//for i := range want {
	//	if !cmp.Equal(got[i], want[i]) {
	//		t.Errorf("unexpected alert during inhibited run -want/+got\n%s", cmp.Diff(want[i], got[i]))
	//	}
	//}
}

func TestServer_AlertListHandlers(t *testing.T) {
	// Setup test TCP server
	ts, err := alerttest.NewTCPServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()

	// Create default config
	c := NewConfig()
	s := OpenServer(c)
	cli := Client(s)
	defer s.Close()

	thl := cli.TopicHandlersLink("test")

	// Number of handlers to create
	n := 3
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("handler%d", i)
		if _, err := cli.CreateTopicHandler(thl, client.TopicHandlerOptions{
			ID:      id,
			Kind:    "tcp",
			Options: map[string]interface{}{"address": ts.Addr},
		}); err != nil {
			t.Fatal(err)
		}
	}

	expHandlers := client.TopicHandlers{
		Link:  client.Link{Relation: client.Self, Href: "/kapacitor/v1/alerts/topics/test/handlers?pattern="},
		Topic: "test",
		Handlers: []client.TopicHandler{
			{
				Link:    client.Link{Relation: client.Self, Href: "/kapacitor/v1/alerts/topics/test/handlers/handler0"},
				ID:      "handler0",
				Kind:    "tcp",
				Options: map[string]interface{}{"address": ts.Addr},
			},
			{
				Link:    client.Link{Relation: client.Self, Href: "/kapacitor/v1/alerts/topics/test/handlers/handler1"},
				ID:      "handler1",
				Kind:    "tcp",
				Options: map[string]interface{}{"address": ts.Addr},
			},
			{
				Link:    client.Link{Relation: client.Self, Href: "/kapacitor/v1/alerts/topics/test/handlers/handler2"},
				ID:      "handler2",
				Kind:    "tcp",
				Options: map[string]interface{}{"address": ts.Addr},
			},
		},
	}

	handlers, err := cli.ListTopicHandlers(thl, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(handlers, expHandlers) {
		t.Errorf("unexpected handlers:\ngot\n%+v\nexp\n%+v\n", handlers, expHandlers)
	}

	// Restart the server
	s.Restart()

	// Check again
	handlers, err = cli.ListTopicHandlers(thl, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(handlers, expHandlers) {
		t.Errorf("unexpected handlers after restart:\ngot\n%+v\nexp\n%+v\n", handlers, expHandlers)
	}

	var exp client.TopicHandlers

	// Pattern = *
	handlers, err = cli.ListTopicHandlers(thl, &client.ListTopicHandlersOptions{
		Pattern: "*",
	})
	if err != nil {
		t.Fatal(err)
	}
	exp = expHandlers
	exp.Link.Href = "/kapacitor/v1/alerts/topics/test/handlers?pattern=%2A"
	if !reflect.DeepEqual(handlers, exp) {
		t.Errorf("unexpected handlers with pattern \"*\":\ngot\n%+v\nexp\n%+v\n", handlers, exp)
	}

	// Pattern = handler*
	handlers, err = cli.ListTopicHandlers(thl, &client.ListTopicHandlersOptions{
		Pattern: "handler*",
	})
	if err != nil {
		t.Fatal(err)
	}
	exp = expHandlers
	exp.Link.Href = "/kapacitor/v1/alerts/topics/test/handlers?pattern=handler%2A"
	if !reflect.DeepEqual(handlers, exp) {
		t.Errorf("unexpected handlers with pattern \"handler*\":\ngot\n%+v\nexp\n%+v\n", handlers, exp)
	}

	// Pattern = handler0
	handlers, err = cli.ListTopicHandlers(thl, &client.ListTopicHandlersOptions{
		Pattern: "handler0",
	})
	if err != nil {
		t.Fatal(err)
	}
	exp = expHandlers
	exp.Link.Href = "/kapacitor/v1/alerts/topics/test/handlers?pattern=handler0"
	exp.Handlers = expHandlers.Handlers[0:1]
	if !reflect.DeepEqual(handlers, exp) {
		t.Errorf("unexpected handlers with pattern \"handler0\":\ngot\n%+v\nexp\n%+v\n", handlers, exp)
	}
}

func TestServer_AlertTopic(t *testing.T) {
	// Create default config
	c := NewConfig()
	s := OpenServer(c)
	cli := Client(s)
	defer s.Close()

	if _, err := cli.CreateTopicHandler(cli.TopicHandlersLink("misc"), client.TopicHandlerOptions{
		ID:      "testAlertHandler",
		Kind:    "tcp",
		Options: map[string]interface{}{"address": "localhost:4657"},
	}); err != nil {
		t.Fatal(err)
	}

	expTopic := client.Topic{
		Link:         client.Link{Relation: client.Self, Href: "/kapacitor/v1/alerts/topics/misc"},
		ID:           "misc",
		Level:        "OK",
		Collected:    0,
		EventsLink:   client.Link{Relation: "events", Href: "/kapacitor/v1/alerts/topics/misc/events"},
		HandlersLink: client.Link{Relation: "handlers", Href: "/kapacitor/v1/alerts/topics/misc/handlers"},
	}
	topic, err := cli.Topic(cli.TopicLink("misc"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(topic, expTopic) {
		t.Errorf("unexpected topic:\ngot\n%+v\nexp\n%+v\n", topic, expTopic)
	}
}

func TestServer_AlertListTopics(t *testing.T) {
	// Setup test TCP server
	ts, err := alerttest.NewTCPServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()

	// Create default config
	c := NewConfig()
	s := OpenServer(c)
	cli := Client(s)
	defer s.Close()

	for _, topic := range []string{"system", "misc", "test"} {
		if _, err := cli.CreateTopicHandler(cli.TopicHandlersLink(topic), client.TopicHandlerOptions{
			ID:      "testAlertHandler",
			Kind:    "tcp",
			Options: map[string]interface{}{"address": ts.Addr},
		}); err != nil {
			t.Fatal(err)
		}
	}

	expTopics := client.Topics{
		Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/alerts/topics?min-level=OK&pattern="},
		Topics: []client.Topic{
			{
				Link:         client.Link{Relation: client.Self, Href: "/kapacitor/v1/alerts/topics/misc"},
				ID:           "misc",
				Level:        "OK",
				EventsLink:   client.Link{Relation: "events", Href: "/kapacitor/v1/alerts/topics/misc/events"},
				HandlersLink: client.Link{Relation: "handlers", Href: "/kapacitor/v1/alerts/topics/misc/handlers"},
			},
			{
				Link:         client.Link{Relation: client.Self, Href: "/kapacitor/v1/alerts/topics/system"},
				ID:           "system",
				Level:        "OK",
				EventsLink:   client.Link{Relation: "events", Href: "/kapacitor/v1/alerts/topics/system/events"},
				HandlersLink: client.Link{Relation: "handlers", Href: "/kapacitor/v1/alerts/topics/system/handlers"},
			},
			{
				Link:         client.Link{Relation: client.Self, Href: "/kapacitor/v1/alerts/topics/test"},
				ID:           "test",
				Level:        "OK",
				EventsLink:   client.Link{Relation: "events", Href: "/kapacitor/v1/alerts/topics/test/events"},
				HandlersLink: client.Link{Relation: "handlers", Href: "/kapacitor/v1/alerts/topics/test/handlers"},
			},
		},
	}
	topics, err := cli.ListTopics(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(topics, expTopics) {
		t.Errorf("unexpected topics:\ngot\n%+v\nexp\n%+v\n", topics, expTopics)
	}

	tick := `
stream
	|from()
		.measurement('alert')
	|alert()
		.topic('test')
		.id('id')
		.message('message')
		.details('details')
		.crit(lambda: TRUE)
`

	if _, err := cli.CreateTask(client.CreateTaskOptions{
		ID:   "testAlertHandlers",
		Type: client.StreamTask,
		DBRPs: []client.DBRP{{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		}},
		TICKscript: tick,
		Status:     client.Enabled,
	}); err != nil {
		t.Fatal(err)
	}

	point := "alert value=1 0000000000"
	v := url.Values{}
	v.Add("precision", "s")
	s.MustWrite("mydb", "myrp", point, v)

	// Restart the server
	s.Restart()

	// Update expected topics since we triggered an event.
	expTopics.Topics[2].Level = "CRITICAL"

	// Check again
	topics, err = cli.ListTopics(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(topics, expTopics) {
		t.Errorf("unexpected topics after restart:\ngot\n%+v\nexp\n%+v\n", topics, expTopics)
	}

	var exp client.Topics

	// Pattern = *
	topics, err = cli.ListTopics(&client.ListTopicsOptions{
		Pattern: "*",
	})
	if err != nil {
		t.Fatal(err)
	}
	exp = expTopics
	exp.Link.Href = "/kapacitor/v1/alerts/topics?min-level=OK&pattern=%2A"
	if !reflect.DeepEqual(topics, exp) {
		t.Errorf("unexpected topics with pattern \"*\":\ngot\n%+v\nexp\n%+v\n", topics, exp)
	}

	// Pattern = test
	topics, err = cli.ListTopics(&client.ListTopicsOptions{
		Pattern: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	exp = expTopics
	exp.Link.Href = "/kapacitor/v1/alerts/topics?min-level=OK&pattern=test"
	exp.Topics = expTopics.Topics[2:]
	if !reflect.DeepEqual(topics, exp) {
		t.Errorf("unexpected topics with pattern \"test\":\ngot\n%+v\nexp\n%+v\n", topics, exp)
	}

	// MinLevel = INFO
	topics, err = cli.ListTopics(&client.ListTopicsOptions{
		MinLevel: "INFO",
	})
	if err != nil {
		t.Fatal(err)
	}
	exp = expTopics
	exp.Link.Href = "/kapacitor/v1/alerts/topics?min-level=INFO&pattern="
	exp.Topics = expTopics.Topics[2:]
	if !reflect.DeepEqual(topics, exp) {
		t.Errorf("unexpected topics min level \"info\":\ngot\n%+v\nexp\n%+v\n", topics, exp)
	}
}

func TestServer_AlertHandler_MultipleHandlers(t *testing.T) {
	resultJSON := `{"series":[{"name":"alert","columns":["time","value"],"values":[["1970-01-01T00:00:00Z",1]]}]}`

	// Create default config
	c := NewConfig()

	// Configure slack
	slack := slacktest.NewServer()
	c.Slack[0].Enabled = true
	c.Slack[0].URL = slack.URL + "/test/slack/url"

	// Configure victorops
	vo := victoropstest.NewServer()
	c.VictorOps.Enabled = true
	c.VictorOps.URL = vo.URL
	c.VictorOps.APIKey = "api_key"

	s := OpenServer(c)
	cli := Client(s)
	closed := false
	defer func() {
		if !closed {
			s.Close()
		}
	}()

	if _, err := cli.CreateTopicHandler(cli.TopicHandlersLink("test"), client.TopicHandlerOptions{
		ID:   "testAlertHandlers-VO",
		Kind: "victorops",
		Options: map[string]interface{}{
			"routing-key": "key",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.CreateTopicHandler(cli.TopicHandlersLink("test"), client.TopicHandlerOptions{
		ID:   "testAlertHandlers-Slack",
		Kind: "slack",
		Options: map[string]interface{}{
			"channel": "#test",
		},
	}); err != nil {
		t.Fatal(err)
	}

	tick := `
stream
	|from()
		.measurement('alert')
	|alert()
		.topic('test')
		.id('id')
		.message('message')
		.details('details')
		.crit(lambda: TRUE)
`

	if _, err := cli.CreateTask(client.CreateTaskOptions{
		ID:   "testAlertHandlers",
		Type: client.StreamTask,
		DBRPs: []client.DBRP{{
			Database:        "mydb",
			RetentionPolicy: "myrp",
		}},
		TICKscript: tick,
		Status:     client.Enabled,
	}); err != nil {
		t.Fatal(err)
	}

	point := "alert value=1 0000000000"
	v := url.Values{}
	v.Add("precision", "s")
	s.MustWrite("mydb", "myrp", point, v)

	// Close the entire server to ensure all data is processed
	s.Close()
	closed = true

	// Validate slack
	{
		slack.Close()
		got := slack.Requests()
		exp := []slacktest.Request{{
			URL: "/test/slack/url",
			PostData: slacktest.PostData{
				Channel:  "#test",
				Username: "kapacitor",
				Text:     "",
				Attachments: []slacktest.Attachment{
					{
						Fallback:  "message",
						Color:     "danger",
						Text:      "message",
						Mrkdwn_in: []string{"text"},
					},
				},
			},
		}}
		if !reflect.DeepEqual(exp, got) {
			t.Errorf("unexpected slack request:\nexp\n%+v\ngot\n%+v\n", exp, got)
		}
	}
	// Validate victorops
	{
		vo.Close()
		got := vo.Requests()
		exp := []victoropstest.Request{{
			URL: "/api_key/key",
			PostData: victoropstest.PostData{
				MessageType:    "CRITICAL",
				EntityID:       "id",
				StateMessage:   "message",
				Timestamp:      0,
				MonitoringTool: "kapacitor",
				Data:           resultJSON,
			},
		}}
		if !reflect.DeepEqual(exp, got) {
			t.Errorf("unexpected victorops request:\nexp\n%+v\ngot\n%+v\n", exp, got)
		}
	}
}

func TestStorage_Rebuild(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	storages, err := cli.ListStorage()
	if err != nil {
		t.Fatal(err)
	}

	for _, storage := range storages.Storage {
		t.Log(storage.Link)
		err := cli.DoStorageAction(storage.Link, client.StorageActionOptions{
			Action: client.StorageRebuild,
		})
		if err != nil {
			t.Errorf("error rebuilding storage %q: %v", storage.Name, err)
		}
	}
}

func TestStorage_Backup(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	// Create a task
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

	// Perform backup
	size, r, err := cli.Backup()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	backup, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if got, exp := int64(len(backup)), size; got != exp {
		t.Fatalf("unexpected backup size got %d exp %d", got, exp)
	}

	// Stop the server
	s.Stop()

	// Restore from backup
	if err := ioutil.WriteFile(s.Config.Storage.BoltDBPath, backup, 0644); err != nil {
		t.Fatal(err)
	}

	// Start the server again
	s.Start()

	// Check that the task was restored
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

func TestLoadService(t *testing.T) {
	s, c, cli := OpenLoadServer()

	// If the list of test fixtures changes update this list
	tasks := []string{"base", "cpu_alert", "implicit", "join", "other"}
	ts, err := cli.ListTasks(nil)
	if err != nil {
		t.Fatalf("enountered error listing tasks: %v", err)
	}
	for i, task := range ts {
		if exp, got := tasks[i], task.ID; exp != got {
			t.Fatalf("expected task ID to be %v, got %v\n", exp, got)
		}
	}

	// If the list of test fixtures changes update this list
	templates := []string{"base_template", "implicit_template"}
	tmps, err := cli.ListTemplates(nil)
	if err != nil {
		t.Fatalf("enountered error listing tasks: %v", err)
	}
	for i, template := range tmps {
		if exp, got := templates[i], template.ID; exp != got {
			t.Fatalf("expected template ID to be %v, got %v\n", exp, got)
		}
	}

	// If the list of test fixtures changes update this list
	topicHandlers := []string{"example", "other"}
	link := cli.TopicHandlersLink("cpu")
	ths, err := cli.ListTopicHandlers(link, nil)
	if err != nil {
		t.Fatalf("enountered error listing tasks: %v", err)
	}
	for i, th := range ths.Handlers {
		if exp, got := topicHandlers[i], th.ID; exp != got {
			t.Fatalf("expected topic-handler ID to be %v, got %v\n", exp, got)
		}
	}

	// delete task file
	err = os.Rename(
		path.Join(c.Load.Dir, "tasks", "join.tick"),
		path.Join(c.Load.Dir, "tasks", "z.tick"),
	)
	if err != nil {
		t.Fatalf("failed to rename tickscript: %v", err)
	}

	// reload
	s.Reload()

	// If the list of test fixtures changes update this list
	tasks = []string{"base", "cpu_alert", "implicit", "other", "z"}
	ts, err = cli.ListTasks(nil)
	if err != nil {
		t.Fatalf("enountered error listing tasks: %v", err)
	}
	for i, task := range ts {
		if exp, got := tasks[i], task.ID; exp != got {
			t.Fatalf("expected task ID to be %v, got %v\n", exp, got)
		}
	}

	// rename template file
	err = os.Rename(
		path.Join(c.Load.Dir, "templates", "base_template.tick"),
		path.Join(c.Load.Dir, "templates", "new.tick"),
	)
	if err != nil {
		t.Fatalf("failed to rename tickscript: %v", err)
	}

	// reload
	s.Reload()

	// If the list of test fixtures changes update this list
	templates = []string{"implicit_template", "new"}
	tmps, err = cli.ListTemplates(nil)
	if err != nil {
		t.Fatalf("enountered error listing templates: %v", err)
	}
	for i, template := range tmps {
		if exp, got := templates[i], template.ID; exp != got {
			t.Fatalf("expected template ID to be %v, got %v\n", exp, got)
		}
	}
	// move template file back
	err = os.Rename(
		path.Join(c.Load.Dir, "templates", "new.tick"),
		path.Join(c.Load.Dir, "templates", "base_template.tick"),
	)

	// add a new handler
	f, err := os.Create(path.Join(c.Load.Dir, "handlers", "new.tick"))
	if err != nil {
		t.Fatalf("failed to create new handler file: %v", err)
	}

	script := `topic: cpu
id: new
kind: slack
match: changed() == TRUE
options:
  channel: '#alerts'
`

	if _, err := f.Write([]byte(script)); err != nil {
		t.Fatalf("failed to write handler: %v", err)
	}
	f.Close()

	// remove handler file back
	if err := os.Remove(path.Join(c.Load.Dir, "handlers", "other.yaml")); err != nil {
		t.Fatalf("failed to remove handler file: %v", err)
	}

	// reload
	s.Reload()

	// If the list of test fixtures changes update this list
	topicHandlers = []string{"example", "new"}
	link = cli.TopicHandlersLink("cpu")
	ths, err = cli.ListTopicHandlers(link, nil)
	if err != nil {
		t.Fatalf("enountered error listing topic-handlers: %v", err)
	}
	for i, th := range ths.Handlers {
		if exp, got := topicHandlers[i], th.ID; exp != got {
			t.Fatalf("expected topic-handler ID to be %v, got %v\n", exp, got)
		}
	}

}

func TestSideloadService(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	if err := copyFiles("testdata/sideload", dir); err != nil {
		t.Fatal(err)
	}
	s, cli := OpenDefaultServer()
	defer s.Close()

	id := "testSideloadTask"
	ttype := client.StreamTask
	dbrps := []client.DBRP{{
		Database:        "mydb",
		RetentionPolicy: "myrp",
	}}
	tick := fmt.Sprintf(`stream
	|from()
		.measurement('test')
	|sideload()
		.source('file://%s')
		.order('host/{{.host}}.yml', 'service/{{.service}}.yml', 'region/{{.region}}.yml')
		.field('cpu_usage_idle_warn', 30.0)
		.field('cpu_usage_idle_crit', 15.0)
	|httpOut('sideload')
`, dir)

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

	endpoint := fmt.Sprintf("%s/tasks/%s/sideload", s.URL(), id)

	// Request data before any writes and expect null responses
	nullResponse := `{"series":null}`
	err = s.HTTPGetRetry(endpoint, nullResponse, 100, time.Millisecond*5)
	if err != nil {
		t.Error(err)
	}

	points := `test,host=host002,service=cart,region=us-east-1 value=1 0000000000`
	v := url.Values{}
	v.Add("precision", "s")
	s.MustWrite("mydb", "myrp", points, v)

	exp := `{"series":[{"name":"test","tags":{"host":"host002","region":"us-east-1","service":"cart"},"columns":["time","cpu_usage_idle_crit","cpu_usage_idle_warn","value"],"values":[["1970-01-01T00:00:00Z",4,10,1]]}]}`
	err = s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5)
	if err != nil {
		t.Error(err)
	}

	// Update source file
	host002Override := `
---
cpu_usage_idle_warn: 8
`
	f, err := os.Create(filepath.Join(dir, "host/host002.yml"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(f, strings.NewReader(host002Override))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// reload
	s.Reload()

	// Write new points
	points = `test,host=host002,service=cart,region=us-east-1 value=2 0000000001`
	s.MustWrite("mydb", "myrp", points, v)

	exp = `{"series":[{"name":"test","tags":{"host":"host002","region":"us-east-1","service":"cart"},"columns":["time","cpu_usage_idle_crit","cpu_usage_idle_warn","value"],"values":[["1970-01-01T00:00:01Z",5,8,2]]}]}`
	err = s.HTTPGetRetry(endpoint, exp, 100, time.Millisecond*5)
	if err != nil {
		t.Error(err)
	}
}

func TestLogSessions_HeaderJSON(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	u := cli.BaseURL()
	u.Path = "/logs"
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		t.Fatal(err)
		return
	}

	req.Header.Add("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer resp.Body.Close()

	if exp, got := "application/json; charset=utf-8", resp.Header.Get("Content-Type"); exp != got {
		t.Fatalf("expected: %v, got: %v\n", exp, got)
		return
	}

}

func TestLogSessions_HeaderGzip(t *testing.T) {
	s, cli := OpenDefaultServer()
	defer s.Close()

	u := cli.BaseURL()
	u.Path = "/logs"
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		t.Fatal(err)
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer resp.Body.Close()

	if exp, got := "", resp.Header.Get("Content-Encoding"); exp != got {
		t.Fatalf("expected: %v, got: %v\n", exp, got)
		return
	}

}

func compareListIgnoreOrder(got, exp []interface{}, cmpF func(got, exp interface{}) bool) error {
	if len(got) != len(exp) {
		return fmt.Errorf("unequal lists ignoring order:\ngot\n%s\nexp\n%s\n", spew.Sdump(got), spew.Sdump(exp))
	}

	if cmpF == nil {
		cmpF = func(got, exp interface{}) bool {
			if !reflect.DeepEqual(got, exp) {
				return false
			}
			return true
		}
	}

	for _, e := range exp {
		found := false
		for _, g := range got {
			if cmpF(g, e) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("unequal lists ignoring order:\ngot\n%s\nexp\n%s\n", spew.Sdump(got), spew.Sdump(exp))
		}
	}
	return nil
}
