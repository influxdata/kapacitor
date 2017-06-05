package integrations

//------------------------------------------------
// General Kapacitor Benchmarks
//
// There are a few basic dimensions being tested with these benchmarks:
//
// 1. Number of Tasks
// 2. Number of Points
//
// Each benchmark test is designed to vary each dimension in isolation,
// so as to understand how changes to Kapacitor effect the complete spectrum of use cases.
//
// As such each benchmark method is named Benchmark<DESCRIPTION>_T<TASK_COUNT>_P<POINT_COUNT>

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/logging/loggingtest"
	"github.com/influxdata/kapacitor/services/noauth"
)

const (
	simpleM1Task = `stream
	|from()
		.measurement('m1')
`
	countM1Task = `stream
	|from()
		.measurement('m1')
	|window()
		.period(100s)
		.every(100s)
	|count('value')
`

	alertM1Task = `stream
	|from()
		.measurement('m1')
	|alert()
		.id('{{ .Name }}_{{ .TaskName }}')
		.info(lambda: "value" > 10)
		.warn(lambda: "value" > 20)
		.crit(lambda: "value" > 30)
`
	joinM12Task = `
var m1 = stream
	|from()
		.measurement('m1')
var m2 = stream
	|from()
		.measurement('m2')

m1
	|join(m2)
		.as('m1','m2')
		.tolerance(1s)
	|eval(lambda: "m1.value" * "m2.value")
		.as('value')
`
)

//----------------------------
// Match vs NoMatch Benchmarks

func BenchmarkSimpleTask_T1000_P5000_Matches(b *testing.B) {
	Bench(b, 1000, 5000, 5000, simpleM1Task, "dbname", "rpname", "m1")
}

func BenchmarkSimpleTask_T1000_P5000_NoMatches(b *testing.B) {
	Bench(b, 1000, 5000, 0, simpleM1Task, "dbname", "rpname", "m2")
}

func BenchmarkSimpleTask_T100_P5000_Matches(b *testing.B) {
	Bench(b, 100, 5000, 5000, simpleM1Task, "dbname", "rpname", "m1")
}

func BenchmarkSimpleTask_T100_P5000_NoMatches(b *testing.B) {
	Bench(b, 100, 5000, 0, simpleM1Task, "dbname", "rpname", "m2")
}

func BenchmarkSimpleTask_T10_P5000_Matches(b *testing.B) {
	Bench(b, 10, 5000, 5000, simpleM1Task, "dbname", "rpname", "m1")
}

func BenchmarkSimpleTask_T10_P5000_NoMatches(b *testing.B) {
	Bench(b, 10, 5000, 0, simpleM1Task, "dbname", "rpname", "m2")
}

//----------------------------
// Count Task Benchmarks

// Few tasks, few points
func BenchmarkCountTask_T10_P500(b *testing.B) {
	Bench(b, 10, 500, 500, countM1Task, "dbname", "rpname", "m1")
}

// Few tasks, many points
func BenchmarkCountTask_T10_P50000(b *testing.B) {
	Bench(b, 10, 50000, 50000, countM1Task, "dbname", "rpname", "m1")
}

// Many tasks, few points
func BenchmarkCountTask_T1000_P500(b *testing.B) {
	Bench(b, 1000, 500, 500, countM1Task, "dbname", "rpname", "m1")
}

// Many tasks, many points
func BenchmarkCountTask_T100_P5000(b *testing.B) {
	Bench(b, 100, 5000, 5000, countM1Task, "dbname", "rpname", "m1")
}

//----------------------------
// Alert Task Benchmarks

// Few tasks, few points
func BenchmarkAlertTask_T10_P500(b *testing.B) {
	Bench(b, 10, 500, 500, alertM1Task, "dbname", "rpname", "m1")
}

// Few tasks, many points
func BenchmarkAlertTask_T10_P50000(b *testing.B) {
	Bench(b, 10, 50000, 50000, alertM1Task, "dbname", "rpname", "m1")
}

// Many tasks, few points
func BenchmarkAlertTask_T1000_P500(b *testing.B) {
	Bench(b, 1000, 500, 500, alertM1Task, "dbname", "rpname", "m1")
}

// Many tasks, many points
func BenchmarkAlertTask_T100_P5000(b *testing.B) {
	Bench(b, 100, 5000, 5000, alertM1Task, "dbname", "rpname", "m1")
}

//----------------------------
// Join Task Benchmarks

// Few tasks, few points
func BenchmarkJoinTask_T10_P500(b *testing.B) {
	Bench(b, 10, 500, 1000, joinM12Task, "dbname", "rpname", "m1", "m2")
}

// Few tasks, many points
func BenchmarkJoinTask_T10_P50000(b *testing.B) {
	Bench(b, 10, 50000, 100000, joinM12Task, "dbname", "rpname", "m1", "m2")
}

// Many tasks, few points
func BenchmarkJoinTask_T1000_P500(b *testing.B) {
	Bench(b, 1000, 500, 1000, joinM12Task, "dbname", "rpname", "m1", "m2")
}

// Many tasks, many points
func BenchmarkJoinTask_T100_P5000(b *testing.B) {
	Bench(b, 100, 5000, 10000, joinM12Task, "dbname", "rpname", "m1", "m2")
}

// Generic Benchmark method
func Bench(b *testing.B, tasksCount, pointCount, expectedProcessedCount int, tickScript, db, rp string, measurements ...string) {
	// Setup HTTPD service
	config := httpd.NewConfig()
	config.BindAddress = ":0" // Choose port dynamically
	config.LogEnabled = false
	httpdService := httpd.NewService(config, "localhost", logService.NewLogger("[http] ", log.LstdFlags), logService)

	httpdService.Handler.AuthService = noauth.NewService(logService.NewLogger("[noauth] ", log.LstdFlags))
	err := httpdService.Open()
	if err != nil {
		b.Fatal(err)
	}

	writes := make([]struct {
		request *http.Request
		seeker  io.Seeker
	}, len(measurements))

	for i, m := range measurements {
		writes[i].request, writes[i].seeker = createWriteRequest(b, db, rp, m, pointCount)
	}

	dbrps := []kapacitor.DBRP{{Database: db, RetentionPolicy: rp}}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Do not time setup
		b.StopTimer()
		tm := kapacitor.NewTaskMaster("bench", newServerInfo(), loggingtest.New())
		tm.HTTPDService = httpdService
		tm.UDFService = nil
		tm.TaskStore = taskStore{}
		tm.DeadmanService = deadman{}
		tm.Open()

		httpdService.Handler.PointsWriter = tm
		tasks := createTasks(b, tm, tasksCount, tickScript, dbrps)

		// Seek writes back to beginning
		for _, write := range writes {
			write.seeker.Seek(0, 0)
		}

		wg := sync.WaitGroup{}
		wg.Add(len(writes))

		// Time how long it takes to process all data
		b.StartTimer()
		for _, write := range writes {
			go func(writeRequest *http.Request, seeker io.Seeker) {
				defer wg.Done()
				responseRecorder := httptest.NewRecorder()
				httpdService.Handler.ServeHTTP(responseRecorder, writeRequest)
				if responseRecorder.Code != http.StatusNoContent {
					b.Fatalf("failed to write test data %s", responseRecorder.Body.String())
				}
			}(write.request, write.seeker)
		}

		wg.Wait()

		tm.Drain()
		for _, t := range tasks {
			t.Wait()
		}

		// Do not time cleanup
		b.StopTimer()
		// Validate that tasks did not error out and processed all points
		validateTasks(b, tm, tasks, expectedProcessedCount)

		tm.Close()
	}
}

func createTasks(tb testing.TB, tm *kapacitor.TaskMaster, count int, tickScript string, dbrps []kapacitor.DBRP) []*kapacitor.ExecutingTask {
	tasks := make([]*kapacitor.ExecutingTask, count)
	for i := 0; i < count; i++ {
		task, err := tm.NewTask(
			fmt.Sprintf("task_%v", i),
			tickScript,
			kapacitor.StreamTask,
			dbrps,
			0,
			nil,
		)
		if err != nil {
			tb.Fatal(err)
		}

		tasks[i], err = tm.StartTask(task)
		if err != nil {
			tb.Fatal(err)
		}
	}
	return tasks
}

func validateTasks(tb testing.TB, tm *kapacitor.TaskMaster, tasks []*kapacitor.ExecutingTask, expectedProcessedCount int) {
	for _, task := range tasks {
		if !tm.IsExecuting(task.Task.ID) {
			tb.Fatalf("task %s failed", task.Task.ID)
		}
		stats, err := tm.ExecutionStats(task.Task.ID)
		if err != nil {
			tb.Fatal(err)
		}
		if got, exp := stats.NodeStats["stream0"]["collected"], int64(expectedProcessedCount); got != exp {
			tb.Fatalf("task %s didn't process correct amount of points: got %d exp %d", task.Task.ID, got, exp)
		}
	}
}

// create an HTTP Request and a Seeker that can be used to reset the request back to the beginning.
func createWriteRequest(tb testing.TB, db, rp, measurement string, pointCount int) (*http.Request, io.Seeker) {
	uri := "/write?"

	params := url.Values{}
	params.Set("precision", "s")
	params.Set("db", db)
	params.Set("rp", rp)

	var data bytes.Buffer
	for i := 0; i < pointCount; i++ {
		fmt.Fprintf(&data, "%s value=%d %010d\n", measurement, i, i)
	}

	reader := bytes.NewReader(data.Bytes())

	req, err := http.NewRequest("POST", uri+params.Encode(), reader)
	if err != nil {
		tb.Fatal(err)
	}

	return req, reader
}
