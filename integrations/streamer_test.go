package integrations

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"
	"text/template"
	"time"

	"github.com/influxdb/influxdb/client"
	imodels "github.com/influxdb/influxdb/models"
	"github.com/influxdb/kapacitor"
	"github.com/influxdb/kapacitor/clock"
	"github.com/influxdb/kapacitor/services/httpd"
	"github.com/influxdb/kapacitor/wlog"
)

var httpService *httpd.Service
var logService = &LogService{}

var dbrps = []kapacitor.DBRP{
	{
		Database:        "dbname",
		RetentionPolicy: "rpname",
	},
}

func init() {
	wlog.SetLevel(wlog.OFF)
	// create API server
	config := httpd.NewConfig()
	config.BindAddress = ":0" // Choose port dynamically
	httpService = httpd.NewService(config, logService.NewLogger("[http] ", log.LstdFlags))
	err := httpService.Open()
	if err != nil {
		panic(err)
	}
}

func TestStream_Window(t *testing.T) {

	var script = `
stream
	.from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	.window()
		.period(10s)
		.every(10s)
	.httpOut('TestStream_Window')
`

	nums := []float64{
		97.1,
		92.6,
		95.6,
		93.1,
		92.6,
		95.8,
		92.7,
		96.0,
		93.4,
		95.3,
	}

	values := make([][]interface{}, len(nums))
	for i, num := range nums {
		values[i] = []interface{}{
			time.Date(1971, 1, 1, 0, 0, i, 0, time.UTC),
			"serverA",
			"idle",
			num,
		}
	}

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "host", "type", "value"},
				Values:  values,
			},
		},
	}

	clock, et, errCh, tm := testStreamer(t, "TestStream_Window", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(13 * time.Second))
	// Wait till the replay has finished
	if e := <-errCh; e != nil {
		t.Error(e)
	}
	// Wait till the task is finished
	if e := et.Err(); e != nil {
		t.Error(e)
	}

	// Get the result
	output, err := et.GetOutput("TestStream_Window")
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(output.Endpoint())
	if err != nil {
		t.Fatal(err)
	}

	// Assert we got the expected result
	result := kapacitor.ResultFromJSON(resp.Body)
	if eq, msg := compareResults(er, result); !eq {
		t.Error(msg)
	}
}

func TestStream_SimpleMR(t *testing.T) {

	var script = `
stream
	.from().measurement('cpu')
	.where(lambda: "host" == 'serverA')
	.window()
		.period(10s)
		.every(10s)
	.mapReduce(influxql.count('value'))
	.httpOut('TestStream_SimpleMR')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	clock, et, errCh, tm := testStreamer(t, "TestStream_SimpleMR", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(15 * time.Second))
	// Wait till the replay has finished
	if e := <-errCh; e != nil {
		t.Error(e)
	}
	// Wait till the task is finished
	if e := et.Err(); e != nil {
		t.Error(e)
	}

	// Get the result
	output, err := et.GetOutput("TestStream_SimpleMR")
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(output.Endpoint())
	if err != nil {
		t.Fatal(err)
	}

	// Assert we got the expected result
	result := kapacitor.ResultFromJSON(resp.Body)
	if eq, msg := compareResults(er, result); !eq {
		t.Error(msg)
	}
}

func TestStream_GroupBy(t *testing.T) {

	var script = `
stream
	.from().measurement('errors')
	.groupBy('service')
	.window()
		.period(10s)
		.every(10s)
	.mapReduce(influxql.sum('value'))
	.httpOut('error_count')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "errors",
				Tags:    map[string]string{"service": "cartA"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					47.0,
				}},
			},
			{
				Name:    "errors",
				Tags:    map[string]string{"service": "login"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					45.0,
				}},
			},
			{
				Name:    "errors",
				Tags:    map[string]string{"service": "front"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
					32.0,
				}},
			},
		},
	}

	clock, et, errCh, tm := testStreamer(t, "TestStream_GroupBy", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(13 * time.Second))
	// Wait till the replay has finished
	if e := <-errCh; e != nil {
		t.Error(e)
	}
	// Wait till the task is finished
	if e := et.Err(); e != nil {
		t.Error(e)
	}

	// Get the result
	output, err := et.GetOutput("error_count")
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(output.Endpoint())
	if err != nil {
		t.Fatal(err)
	}

	// Assert we got the expected result
	result := kapacitor.ResultFromJSON(resp.Body)
	if eq, msg := compareResults(er, result); !eq {
		t.Error(msg)
	}
}

func TestStream_Join(t *testing.T) {

	var script = `
var errorCounts = stream
			.from().measurement('errors')
			.groupBy('service')
			.window()
				.period(10s)
				.every(10s)
				.align()
			.mapReduce(influxql.sum('value'))

var viewCounts = stream
			.from().measurement('views')
			.groupBy('service')
			.window()
				.period(10s)
				.every(10s)
				.align()
			.mapReduce(influxql.sum('value'))

errorCounts.join(viewCounts)
		.as('errors', 'views')
		.streamName('error_view')
	.eval(lambda: "errors.sum" / "views.sum")
		.as('error_percent')
		.keep()
	.httpOut('error_rate')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "cartA"},
				Columns: []string{"time", "error_percent", "errors.sum", "views.sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.01,
					47.0,
					4700.0,
				}},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "login"},
				Columns: []string{"time", "error_percent", "errors.sum", "views.sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.01,
					45.0,
					4500.0,
				}},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "front"},
				Columns: []string{"time", "error_percent", "errors.sum", "views.sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.01,
					32.0,
					3200.0,
				}},
			},
		},
	}

	clock, et, errCh, tm := testStreamer(t, "TestStream_Join", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(13 * time.Second))
	// Wait till the replay has finished
	if e := <-errCh; e != nil {
		t.Error(e)
	}
	// Wait till the task is finished
	if e := et.Err(); e != nil {
		t.Error(e)
	}

	// Get the result
	output, err := et.GetOutput("error_rate")
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(output.Endpoint())
	if err != nil {
		t.Fatal(err)
	}

	// Assert we got the expected result
	result := kapacitor.ResultFromJSON(resp.Body)
	if eq, msg := compareResultsIgnoreSeriesOrder(er, result); !eq {
		t.Error(msg)
	}
}

func TestStream_JoinTolerance(t *testing.T) {

	var script = `
var errorCounts = stream
			.from().measurement('errors')
			.groupBy('service')

var viewCounts = stream
			.from().measurement('views')
			.groupBy('service')

errorCounts.join(viewCounts)
		.as('errors', 'views')
		.tolerance(2s)
		.streamName('error_view')
	.eval(lambda: "errors.value" / "views.value")
		.as('error_percent')
	.window()
		.period(10s)
		.every(10s)
	.mapReduce(influxql.mean('error_percent'))
		.as('error_percent')
	.httpOut('error_rate')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "cartA"},
				Columns: []string{"time", "error_percent"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.01,
				}},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "login"},
				Columns: []string{"time", "error_percent"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.01,
				}},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "front"},
				Columns: []string{"time", "error_percent"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 12, 0, time.UTC),
					0.01,
				}},
			},
		},
	}

	clock, et, errCh, tm := testStreamer(t, "TestStream_JoinTolerance", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(13 * time.Second))
	// Wait till the replay has finished
	if e := <-errCh; e != nil {
		t.Error(e)
	}
	// Wait till the task is finished
	if e := et.Err(); e != nil {
		t.Error(e)
	}

	// Get the result
	output, err := et.GetOutput("error_rate")
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(output.Endpoint())
	if err != nil {
		t.Fatal(err)
	}

	// Assert we got the expected result
	result := kapacitor.ResultFromJSON(resp.Body)
	if eq, msg := compareResultsIgnoreSeriesOrder(er, result); !eq {
		t.Error(msg)
	}
}

func TestStream_JoinFill(t *testing.T) {
	var script = `
var errorCounts = stream
			.from().measurement('errors')
			.groupBy('service')

var viewCounts = stream
			.from().measurement('views')
			.groupBy('service')

errorCounts.join(viewCounts)
		.as('errors', 'views')
		.fill(0.0)
		.streamName('error_view')
	.eval(lambda:  "errors.value" + "views.value")
		.as('error_percent')
	.window()
		.period(10s)
		.every(10s)
	.mapReduce(influxql.count('error_percent'))
	.httpOut('error_rate')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "cartA"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					7.0,
				}},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "login"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					7.0,
				}},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "front"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					8.0,
				}},
			},
		},
	}

	clock, et, errCh, tm := testStreamer(t, "TestStream_JoinFill", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(13 * time.Second))
	// Wait till the replay has finished
	if e := <-errCh; e != nil {
		t.Error(e)
	}
	// Wait till the task is finished
	if e := et.Err(); e != nil {
		t.Error(e)
	}

	// Get the result
	output, err := et.GetOutput("error_rate")
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(output.Endpoint())
	if err != nil {
		t.Fatal(err)
	}

	// Assert we got the expected result
	result := kapacitor.ResultFromJSON(resp.Body)
	if eq, msg := compareResultsIgnoreSeriesOrder(er, result); !eq {
		t.Error(msg)
	}
}

func TestStream_JoinN(t *testing.T) {

	var script = `
var cpu = stream
			.from().measurement('cpu')
			.where(lambda: "cpu" == 'total')
var mem = stream
			.from().measurement('memory')
			.where(lambda: "type" == 'free')
var disk = stream
			.from().measurement('disk')
			.where(lambda: "device" == 'sda')

cpu.join(mem, disk)
		.as('cpu', 'mem', 'disk')
		.streamName('magic')
		.fill(0.0)
		.window()
			.period(10s)
			.every(10s)
		.mapReduce(influxql.count('cpu.value'))
		.httpOut('all')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "magic",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					9.0,
				}},
			},
		},
	}

	clock, et, errCh, tm := testStreamer(t, "TestStream_JoinN", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(15 * time.Second))
	// Wait till the replay has finished
	if e := <-errCh; e != nil {
		t.Error(e)
	}
	// Wait till the task is finished
	if e := et.Err(); e != nil {
		t.Error(e)
	}

	// Get the result
	output, err := et.GetOutput("all")
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(output.Endpoint())
	if err != nil {
		t.Fatal(err)
	}

	// Assert we got the expected result
	result := kapacitor.ResultFromJSON(resp.Body)
	if eq, msg := compareResults(er, result); !eq {
		t.Error(msg)
	}
}

func TestStream_Union(t *testing.T) {

	var script = `
var cpu = stream
			.from().measurement('cpu')
			.where(lambda: "cpu" == 'total')
var mem = stream
			.from().measurement('memory')
			.where(lambda: "type" == 'free')
var disk = stream
			.from().measurement('disk')
			.where(lambda: "device" == 'sda')

cpu.union(mem, disk)
		.rename('cpu_mem_disk')
		.window()
			.period(10s)
			.every(10s)
		.mapReduce(influxql.count('value'))
		.httpOut('all')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_mem_disk",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					24.0,
				}},
			},
		},
	}

	clock, et, errCh, tm := testStreamer(t, "TestStream_Union", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(15 * time.Second))
	// Wait till the replay has finished
	if e := <-errCh; e != nil {
		t.Error(e)
	}
	// Wait till the task is finished
	if e := et.Err(); e != nil {
		t.Error(e)
	}

	// Get the result
	output, err := et.GetOutput("all")
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(output.Endpoint())
	if err != nil {
		t.Fatal(err)
	}

	// Assert we got the expected result
	result := kapacitor.ResultFromJSON(resp.Body)
	if eq, msg := compareResults(er, result); !eq {
		t.Error(msg)
	}
}

func TestStream_Aggregations(t *testing.T) {

	type testCase struct {
		Method        string
		Args          string
		ER            kapacitor.Result
		UsePointTimes bool
	}

	var scriptTmpl = `
stream
	.from().measurement('cpu')
	.where(lambda: "host" == 'serverA')
	.window()
		.period(10s)
		.every(10s)
	.mapReduce({{ .Method }}({{ .Args }}))
		{{ if .UsePointTimes }}.usePointTimes(){{ end }}
	.httpOut('{{ .Method }}')
`
	endTime := time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC)
	testCases := []testCase{
		testCase{
			Method: "influxql.sum",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "sum"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							940.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "influxql.count",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "count"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							10.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "influxql.distinct",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "distinct"},
						Values: [][]interface{}{
							{
								endTime,
								91.0,
							},
							{
								endTime,
								92.0,
							},
							{
								endTime,
								93.0,
							},
							{
								endTime,
								95.0,
							},
							{
								endTime,
								96.0,
							},
							{
								endTime,
								98.0,
							},
						},
					},
				},
			},
		},
		testCase{
			Method: "influxql.mean",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "mean"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							94.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "influxql.median",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "median"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							94.0,
						}},
					},
				},
			},
		},
		testCase{
			Method:        "influxql.min",
			UsePointTimes: true,
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "min"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
							91.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "influxql.min",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "min"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							91.0,
						}},
					},
				},
			},
		},
		testCase{
			Method:        "influxql.max",
			UsePointTimes: true,
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "max"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
							98.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "influxql.max",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "max"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							98.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "influxql.spread",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "spread"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							7.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "influxql.stddev",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "stddev"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							2.160246899469287,
						}},
					},
				},
			},
		},
		testCase{
			Method:        "influxql.first",
			UsePointTimes: true,
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "first"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
							98.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "influxql.first",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "first"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							98.0,
						}},
					},
				},
			},
		},
		testCase{
			Method:        "influxql.last",
			UsePointTimes: true,
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "last"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
							95.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "influxql.last",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "last"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							95.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "influxql.percentile",
			Args:   "'value', 50.0",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "percentile"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							93.0,
						}},
					},
				},
			},
		},
		testCase{
			Method:        "influxql.top",
			UsePointTimes: true,
			Args:          "2, 'value'",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "host", "top", "type"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
								"serverA",
								98.0,
								"idle",
							},
							{
								time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
								"serverA",
								96.0,
								"idle",
							},
						},
					},
				},
			},
		},
		testCase{
			Method: "influxql.top",
			Args:   "2, 'value'",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "host", "top", "type"},
						Values: [][]interface{}{
							{
								endTime,
								"serverA",
								98.0,
								"idle",
							},
							{
								endTime,
								"serverA",
								96.0,
								"idle",
							},
						},
					},
				},
			},
		},
		testCase{
			Method:        "influxql.bottom",
			UsePointTimes: true,
			Args:          "3, 'value'",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "bottom", "host", "type"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
								91.0,
								"serverA",
								"idle",
							},
							{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
								92.0,
								"serverA",
								"idle",
							},
							{
								time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
								92.0,
								"serverA",
								"idle",
							},
						},
					},
				},
			},
		},
		testCase{
			Method: "influxql.bottom",
			Args:   "3, 'value'",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "bottom", "host", "type"},
						Values: [][]interface{}{
							{
								endTime,
								91.0,
								"serverA",
								"idle",
							},
							{
								endTime,
								92.0,
								"serverA",
								"idle",
							},
							{
								endTime,
								92.0,
								"serverA",
								"idle",
							},
						},
					},
				},
			},
		},
	}

	tmpl, err := template.New("script").Parse(scriptTmpl)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range testCases {
		t.Log("Method:", tc.Method)
		var script bytes.Buffer
		if tc.Args == "" {
			tc.Args = "'value'"
		}
		tmpl.Execute(&script, tc)
		clock, et, errCh, tm := testStreamer(
			t,
			"TestStream_Aggregations",
			string(script.Bytes()),
		)
		defer tm.Close()

		// Move time forward
		clock.Set(clock.Zero().Add(13 * time.Second))
		// Wait till the replay has finished
		if e := <-errCh; e != nil {
			t.Error(e)
		}
		// Wait till the task is finished
		if e := et.Err(); e != nil {
			t.Error(e)
		}

		// Get the result
		output, err := et.GetOutput(tc.Method)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := http.Get(output.Endpoint())
		if err != nil {
			t.Fatal(err)
		}

		// Assert we got the expected result
		result := kapacitor.ResultFromJSON(resp.Body)
		if eq, msg := compareResults(tc.ER, result); !eq {
			t.Error(tc.Method + ": " + msg)
		}

		tm.StopTask(et.Task.Name)
	}
}

func TestStream_CustomFunctions(t *testing.T) {
	t.Skip()
	var script = `
var fMap = loadMapFunc('./TestCustomMapFunction.py')
var fReduce = loadReduceFunc('./TestCustomReduceFunction.py')
stream
	.from().measurement('cpu')
	.where(lambda: "host" == 'serverA')
	.window()
		.period(1s)
		.every(1s)
	.map(fMap, 'idle')
	.reduce(fReduce)
	.cache()
`

	//er := kapacitor.Result{}

	testStreamer(t, "TestStream_CustomFunctions", script)
}

func TestStream_CustomMRFunction(t *testing.T) {
	t.Skip()
	var script = `
var fMapReduce = loadMapReduceFunc('./TestCustomMapReduceFunction.py')
stream
	.from().measurement('cpu')
	.where(lambda: "host" = 'serverA')
	.window()
		.period(1s)
		.every(1s)
	.mapReduce(fMap, 'idle')
	.cache()
`

	//er := kapacitor.Result{}

	testStreamer(t, "TestStream_CustomMRFunction", script)
}

func TestStream_Alert(t *testing.T) {

	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ans, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		requestCount++
		expAns := `{"level":"CRITICAL","data":{"Series":[{"name":"cpu","columns":["time","count"],"values":[["1971-01-01T00:00:10Z",10]]}],"Err":null}}`
		if string(ans) != expAns {
			t.Errorf("got %v exp %v", string(ans), expAns)
		}
	}))
	defer ts.Close()

	var script = `
stream
	.from().measurement('cpu')
	.where(lambda: "host" == 'serverA')
	.window()
		.period(10s)
		.every(10s)
	.mapReduce(influxql.count('idle'))
	.alert()
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.post('` + ts.URL + `')
`

	clock, et, errCh, tm := testStreamer(t, "TestStream_Alert", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(13 * time.Second))
	// Wait till the replay has finished
	if e := <-errCh; e != nil {
		t.Error(e)
	}
	// Wait till the task is finished
	if e := et.Err(); e != nil {
		t.Error(e)
	}

	if requestCount != 1 {
		t.Errorf("got %v exp %v", requestCount, 1)
	}
}

func TestStream_AlertSigma(t *testing.T) {

	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ans, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		requestCount++
		expAns := `{"level":"INFO","data":{"Series":[{"name":"cpu","tags":{"host":"serverA","type":"idle"},"columns":["time","sigma","value"],"values":[["1971-01-01T00:00:07Z",2.469916402324427,16]]}],"Err":null}}`
		if string(ans) != expAns {
			t.Errorf("got %v exp %v", string(ans), expAns)
		}
	}))
	defer ts.Close()

	var script = `
stream
	.from().measurement('cpu')
	.where(lambda: "host" == 'serverA')
	.eval(lambda: sigma("value"))
		.as('sigma')
		.keep()
	.alert()
		.info(lambda: "sigma" > 2.0)
		.warn(lambda: "sigma" > 3.0)
		.crit(lambda: "sigma" > 3.5)
		.post('` + ts.URL + `')
`

	clock, et, errCh, tm := testStreamer(t, "TestStream_AlertSigma", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(13 * time.Second))
	// Wait till the replay has finished
	if e := <-errCh; e != nil {
		t.Error(e)
	}
	// Wait till the task is finished
	if e := et.Err(); e != nil {
		t.Error(e)
	}

	if requestCount != 1 {
		t.Errorf("got %v exp %v", requestCount, 1)
	}
}

func TestStream_AlertComplexWhere(t *testing.T) {

	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ans, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		requestCount++
		expAns := `{"level":"CRITICAL","data":{"Series":[{"name":"cpu","tags":{"host":"serverA","type":"idle"},"columns":["time","value"],"values":[["1971-01-01T00:00:07Z",16]]}],"Err":null}}`
		if string(ans) != expAns {
			t.Errorf("unexpected result:\ngot %v\nexp %v", string(ans), expAns)
		}
	}))
	defer ts.Close()

	var script = `
stream
	.from().measurement('cpu')
	.where(lambda: "host" == 'serverA' AND sigma("value") > 2)
	.alert()
		.crit(lambda: TRUE)
		.post('` + ts.URL + `')
`

	clock, et, errCh, tm := testStreamer(t, "TestStream_AlertComplexWhere", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(13 * time.Second))
	// Wait till the replay has finished
	if e := <-errCh; e != nil {
		t.Error(e)
	}
	// Wait till the task is finished
	if e := et.Err(); e != nil {
		t.Error(e)
	}

	if requestCount != 1 {
		t.Errorf("got %v exp %v", requestCount, 1)
	}
}

func TestStream_AlertFlapping(t *testing.T) {

	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
	}))
	defer ts.Close()
	var script = `
stream
	.from().measurement('cpu')
	.where(lambda: "host" == 'serverA')
	.alert()
		.info(lambda: "value" < 95)
		.warn(lambda: "value" < 94)
		.crit(lambda: "value" < 93)
		.flapping(0.25, 0.50)
		.post('` + ts.URL + `')
`

	clock, et, errCh, tm := testStreamer(t, "TestStream_AlertFlapping", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(13 * time.Second))
	// Wait till the replay has finished
	if e := <-errCh; e != nil {
		t.Error(e)
	}
	// Wait till the task is finished
	if e := et.Err(); e != nil {
		t.Error(e)
	}

	// Flapping detection should drop the last alert.
	if requestCount != 5 {
		t.Errorf("got %v exp %v", requestCount, 5)
	}
}

func TestStream_InfluxDBOut(t *testing.T) {

	var script = `
stream
	.from().measurement('cpu')
	.where(lambda: "host" == 'serverA')
	.window()
		.period(10s)
		.every(10s)
	.mapReduce(influxql.count('value'))
	.influxDBOut()
		.database('db')
		.retentionPolicy('rp')
		.measurement('m')
		.precision('s')
		.tag('key', 'value')
`
	done := make(chan error, 1)
	var points []imodels.Point
	var database string
	var rp string
	var precision string

	influxdb := NewMockInfluxDBService(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		//Respond
		var data client.Response
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
		//Get request data
		database = r.URL.Query().Get("db")
		rp = r.URL.Query().Get("rp")
		precision = r.URL.Query().Get("precision")

		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			done <- err
			return
		}
		points, err = imodels.ParsePointsWithPrecision(b, time.Unix(0, 0), precision)
		done <- err
	}))

	clock, et, errCh, tm := testStreamer(t, "TestStream_InfluxDBOut", script)
	tm.InfluxDBService = influxdb
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(15 * time.Second))
	// Wait till the replay has finished
	if e := <-errCh; e != nil {
		t.Error(e)
	}
	// Wait till the task is finished
	if e := et.Err(); e != nil {
		t.Error(e)
	}
	// Wait till we received a request
	if e := <-done; e != nil {
		t.Error(e)
	}

	if database != "db" {
		t.Errorf("got %v exp %v", database, "db")
	}
	if rp != "rp" {
		t.Errorf("got %v exp %v", rp, "rp")
	}
	if precision != "s" {
		t.Errorf("got %v exp %v", precision, "s")
	}
	if 1 != len(points) {
		t.Errorf("got %v exp %v", len(points), 1)
	} else {
		p := points[0]
		if p.Name() != "m" {
			t.Errorf("got %v exp %v", p.Name(), "m")
		}
		if p.Fields()["count"] != 10.0 {
			t.Errorf("got %v exp %v", p.Fields()["count"], 10.0)
		}
		if len(p.Tags()) != 1 {
			t.Errorf("got %v exp %v", len(p.Tags()), 1)
		}
		if p.Tags()["key"] != "value" {
			t.Errorf("got %s exp %s", p.Tags()["key"], "value")
		}
		tm := time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC)
		if !tm.Equal(p.Time()) {
			t.Errorf("times are not equal exp %s got %s", tm, p.Time())
		}
	}
}

func TestStream_TopSelector(t *testing.T) {

	var script = `
var topScores = stream
    .from().measurement('scores')
    // Get the most recent score for each player
    .groupBy('game', 'player')
    .window()
        .period(2s)
        .every(2s)
        .align()
    .mapReduce(influxql.last('value'))
    // Calculate the top 5 scores per game
    .groupBy('game')
    .mapReduce(influxql.top(5, 'last', 'player'))

topScores
    .httpOut('top_scores')

topScores.sample(4s)
    .mapReduce(influxql.count('top'))
    .httpOut('top_scores_sampled')
`

	tw := time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC)
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "scores",
				Tags:    map[string]string{"game": "g0"},
				Columns: []string{"time", "player", "top"},
				Values: [][]interface{}{
					{tw, "p7", 978.0},
					{tw, "p10", 957.0},
					{tw, "p9", 878.0},
					{tw, "p5", 877.0},
					{tw, "p15", 791.0},
				},
			},
			{
				Name:    "scores",
				Tags:    map[string]string{"game": "g1"},
				Columns: []string{"time", "player", "top"},
				Values: [][]interface{}{
					{tw, "p19", 926.0},
					{tw, "p12", 887.0},
					{tw, "p0", 879.0},
					{tw, "p15", 872.0},
					{tw, "p16", 863.0},
				},
			},
		},
	}

	sampleER := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "scores",
				Tags:    map[string]string{"game": "g0"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{{
					time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
					5.0,
				}},
			},
			{
				Name:    "scores",
				Tags:    map[string]string{"game": "g1"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{{
					time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
					5.0,
				}},
			},
		},
	}

	clock, et, errCh, tm := testStreamer(t, "TestStream_TopSelector", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(10 * time.Second))
	// Wait till the replay has finished
	if e := <-errCh; e != nil {
		t.Error(e)
	}
	// Wait till the task is finished
	if e := et.Err(); e != nil {
		t.Error(e)
	}

	// Get the result
	output, err := et.GetOutput("top_scores")
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(output.Endpoint())
	if err != nil {
		t.Fatal(err)
	}

	// Assert we got the expected result
	result := kapacitor.ResultFromJSON(resp.Body)
	if eq, msg := compareResults(er, result); !eq {
		t.Error(msg)
	}

	// Get the result
	output, err = et.GetOutput("top_scores_sampled")
	if err != nil {
		t.Fatal(err)
	}

	resp, err = http.Get(output.Endpoint())
	if err != nil {
		t.Fatal(err)
	}

	// Assert we got the expected result
	result = kapacitor.ResultFromJSON(resp.Body)
	if eq, msg := compareResults(sampleER, result); !eq {
		t.Error(msg)
	}
}

// Helper test function for streamer
func testStreamer(t *testing.T, name, script string) (clock.Setter, *kapacitor.ExecutingTask, <-chan error, *kapacitor.TaskMaster) {
	if testing.Verbose() {
		wlog.SetLevel(wlog.DEBUG)
	} else {
		wlog.SetLevel(wlog.OFF)
	}

	//Create the task
	task, err := kapacitor.NewStreamer(name, script, dbrps)
	if err != nil {
		t.Fatal(err)
	}

	// Load test data
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.Open(path.Join(dir, "data", name+".srpl"))
	if err != nil {
		t.Fatal(err)
	}
	c := clock.New(time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC))
	r := kapacitor.NewReplay(c)

	// Create a new execution env
	tm := kapacitor.NewTaskMaster(logService)
	tm.HTTPDService = httpService
	tm.Open()

	//Start the task
	et, err := tm.StartTask(task)
	if err != nil {
		t.Fatal(err)
	}

	// Replay test data to executor
	errCh := r.ReplayStream(data, tm.Stream, false, "s")

	t.Log(string(et.Task.Dot()))
	return r.Setter, et, errCh, tm
}
