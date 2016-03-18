package integrations

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"testing"

	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/wlog"
)

var httpdService *httpd.Service

var testDbrps = []kapacitor.DBRP{
	{
		Database:        "dbname",
		RetentionPolicy: "rpname",
	},
}

func init() {
	logService := &LogService{}

	wlog.SetLevel(wlog.OFF)
	// create API server
	config := httpd.NewConfig()
	config.BindAddress = ":0" // Choose port dynamically
	httpdService = httpd.NewService(config, logService.NewLogger("[http] ", log.LstdFlags))

	httpdService.Handler.MetaClient = &metaclient{}

	err := httpdService.Open()
	if err != nil {
		panic(err)
	}
}

func Benchmark_Write_MeasurementNameNotMatches_1000(b *testing.B) {
	BenchWrite(b, 1000, `
stream
	.from().measurement('packets_hello')
`)
}

func Benchmark_Write_MeasurementNameMatches_1000(b *testing.B) {
	BenchWrite(b, 1000, `
stream
	.from().measurement('packets')
`)
}

func Benchmark_Write_MeasurementNameNotMatches_100(b *testing.B) {
	BenchWrite(b, 100, `
stream
	.from().measurement('packets_hello')
`)
}

func Benchmark_Write_MeasurementNameMatches_100(b *testing.B) {
	BenchWrite(b, 100, `
stream
	.from().measurement('packets')
`)
}

func Benchmark_Write_MeasurementNameNotMatches_10(b *testing.B) {
	BenchWrite(b, 10, `
stream
	.from().measurement('packets_hello')
`)
}

func Benchmark_Write_MeasurementNameMatches_10(b *testing.B) {
	BenchWrite(b, 10, `
stream
	.from().measurement('packets')
`)
}

func BenchWrite(b *testing.B, tasksCount int, tickScript string) {

	tm := kapacitor.NewTaskMaster(&LogService{})
	tm.HTTPDService = httpdService
	tm.UDFService = nil
	tm.TaskStore = taskStore{}
	tm.DeadmanService = deadman{}
	tm.Open()

	httpdService.Handler.PointsWriter = tm

	CreateTasks(tm, tasksCount, tickScript)

	writeRequest := CreateWriteRequest(b, "packets_benchmark")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		responseRecorder := httptest.NewRecorder()
		httpdService.Handler.ServeHTTP(responseRecorder, writeRequest)
	}

	b.StopTimer()
	tm.Close()
}

func CreateTasks(tm *kapacitor.TaskMaster, count int, tickScript string) {
	for i := 0; i < count; i++ {
		task, err := tm.NewTask(fmt.Sprintf("task_%v", i), tickScript, kapacitor.StreamTask, testDbrps, 0)
		if err != nil {
			panic(err)
		}

		_, err = tm.StartTask(task)
		if err != nil {
			panic(err)
		}

	}

}

func CreateWriteRequest(tb testing.TB, testFile string) *http.Request {

	uri := "/write?"

	param := make(url.Values)
	param["percision"] = []string{"s"}
	param["db"] = []string{testDbrps[0].Database}
	param["rp"] = []string{testDbrps[0].RetentionPolicy}

	dir, err := os.Getwd()
	if err != nil {
		tb.Fatal(err)
	}
	data, err := os.Open(path.Join(dir, "data", testFile+".srpl"))
	if err != nil {
		tb.Fatal(err)
	}

	req, err := http.NewRequest("POST", uri+param.Encode(), data)

	if err != nil {
		tb.Fatal(err)
	}

	return req
}
