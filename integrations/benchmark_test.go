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
// As such each benchmark method is named Benchmark_T<TASK_COUNT>_P<POINT_COUNT>_<DESCRIPTION>

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/services/httpd"
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
	|log()
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
)

//----------------------------
// Match vs NoMatch Benchmarks

func Benchmark_T1000_P5000_Matches(b *testing.B) {
	Bench(b, 1000, 5000, "dbname", "rpname", "m1", simpleM1Task)
}

func Benchmark_T1000_P5000_NoMatches(b *testing.B) {
	Bench(b, 1000, 5000, "dbname", "rpname", "m2", simpleM1Task)
}

func Benchmark_T100_P5000_Matches(b *testing.B) {
	Bench(b, 100, 5000, "dbname", "rpname", "m1", simpleM1Task)
}

func Benchmark_T100_P5000_NoMatches(b *testing.B) {
	Bench(b, 100, 5000, "dbname", "rpname", "m2", simpleM1Task)
}

func Benchmark_T10_P5000_Matches(b *testing.B) {
	Bench(b, 10, 5000, "dbname", "rpname", "m1", simpleM1Task)
}

func Benchmark_T10_P5000_NoMatches(b *testing.B) {
	Bench(b, 10, 5000, "dbname", "rpname", "m2", simpleM1Task)
}

//----------------------------
// Count Task Benchmarks

// Few tasks, few points
func Benchmark_T10_P500_CountTask(b *testing.B) {
	Bench(b, 10, 500, "dbname", "rpname", "m1", countM1Task)
}

// Few tasks, many points
func Benchmark_T10_P50000_CountTask(b *testing.B) {
	Bench(b, 10, 50000, "dbname", "rpname", "m1", countM1Task)
}

// Many tasks, few points
func Benchmark_T1000_P500(b *testing.B) {
	Bench(b, 1000, 500, "dbname", "rpname", "m1", countM1Task)
}

// Many tasks, many points
func Benchmark_T1000_P50000_CountTask(b *testing.B) {
	Bench(b, 1000, 50000, "dbname", "rpname", "m1", countM1Task)
}

//----------------------------
// Alert Task Benchmarks

// Few tasks, few points
func Benchmark_T10_P500_AlertTask(b *testing.B) {
	Bench(b, 10, 500, "dbname", "rpname", "m1", alertM1Task)
}

// Few tasks, many points
func Benchmark_T10_P50000_AlertTask(b *testing.B) {
	Bench(b, 10, 50000, "dbname", "rpname", "m1", alertM1Task)
}

// Many tasks, few points
func Benchmark_T1000_P500_AlertTask(b *testing.B) {
	Bench(b, 1000, 500, "dbname", "rpname", "m1", alertM1Task)
}

// Generic Benchmark method
func Bench(b *testing.B, tasksCount, pointCount int, db, rp, measurement, tickScript string) {
	// Setup HTTPD service
	config := httpd.NewConfig()
	config.BindAddress = ":0" // Choose port dynamically
	httpdService := httpd.NewService(config, logService.NewLogger("[http] ", log.LstdFlags))

	httpdService.Handler.MetaClient = &metaclient{}
	err := httpdService.Open()
	if err != nil {
		b.Fatal(err)
	}

	writeRequest, seeker := createWriteRequest(b, db, rp, measurement, pointCount)

	dbrps := []kapacitor.DBRP{{Database: db, RetentionPolicy: rp}}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Do not time setup
		b.StopTimer()
		tm := kapacitor.NewTaskMaster("bench", &LogService{})
		tm.HTTPDService = httpdService
		tm.UDFService = nil
		tm.TaskStore = taskStore{}
		tm.DeadmanService = deadman{}
		tm.Open()

		httpdService.Handler.PointsWriter = tm
		tasks := createTasks(tm, tasksCount, tickScript, dbrps)

		// Seek writeRequest back to beginning
		seeker.Seek(0, 0)

		// Time how long it takes to process all data
		b.StartTimer()
		responseRecorder := httptest.NewRecorder()
		httpdService.Handler.ServeHTTP(responseRecorder, writeRequest)
		if responseRecorder.Code != http.StatusNoContent {
			b.Fatalf("failed to write test data %s", responseRecorder.Body.String())
		}

		tm.Drain()
		for _, t := range tasks {
			t.Wait()
		}

		// Do not time cleanup
		b.StopTimer()
		tm.Close()
	}
}

func createTasks(tm *kapacitor.TaskMaster, count int, tickScript string, dbrps []kapacitor.DBRP) []*kapacitor.ExecutingTask {
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
			panic(err)
		}

		tasks[i], err = tm.StartTask(task)
		if err != nil {
			panic(err)
		}
	}
	return tasks
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
