package integrations

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
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

func init() {
	wlog.LogLevel = wlog.OFF
	// create API server
	config := httpd.NewConfig()
	config.BindAddress = ":0" // Choose port dynamically
	httpService = httpd.NewService(config)
	err := httpService.Open()
	if err != nil {
		panic(err)
	}
}

func TestStream_Window(t *testing.T) {

	var script = `
stream
	.from('"dbname"."rpname"."cpu"')
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
			time.Date(1970, 1, 1, 0, 0, i, 0, time.UTC),
			num,
		}
	}

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "value"},
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
	.from('cpu')
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
					time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
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
	.from('errors')
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
					time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
					47.0,
				}},
			},
			{
				Name:    "errors",
				Tags:    map[string]string{"service": "login"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
					45.0,
				}},
			},
			{
				Name:    "errors",
				Tags:    map[string]string{"service": "front"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1970, 1, 1, 0, 0, 8, 0, time.UTC),
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
			.fork()
			.from('errors')
			.groupBy('service')
			.window()
				.period(10s)
				.every(10s)
			.mapReduce(influxql.sum('value'))

var viewCounts = stream
			.fork()
			.from('views')
			.groupBy('service')
			.window()
				.period(10s)
				.every(10s)
			.mapReduce(influxql.sum('value'))

errorCounts.join(viewCounts)
		.as('errors', 'views')
		.rename('error_view')
		.eval(lambda: "errors.sum" / "views.sum")
			.as('error_percent')
		.httpOut('error_rate')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "cartA"},
				Columns: []string{"time", "error_percent", "errors.sum", "views.sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
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
					time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
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
					time.Date(1970, 1, 1, 0, 0, 8, 0, time.UTC),
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
	if eq, msg := compareResults(er, result); !eq {
		t.Error(msg)
	}
}

func TestStream_Union(t *testing.T) {

	var script = `
var cpu = stream
			.fork()
			.from('cpu')
			.where(lambda: "cpu" == 'total')
var mem = stream
			.fork()
			.from('memory')
			.where(lambda: "type" == 'free')
var disk = stream
			.fork()
			.from('disk')
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
					time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
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
		Method string
		Args   string
		ER     kapacitor.Result
	}

	var scriptTmpl = `
stream
	.from('cpu')
	.where(lambda: "host" == 'serverA')
	.window()
		.period(10s)
		.every(10s)
	.mapReduce({{ .Method }}('value' {{ .Args }}))
	.httpOut('{{ .Method }}')
`
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
							time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
							941.0,
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
							time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
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
						Values: [][]interface{}{[]interface{}{
							time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
							[]interface{}{92.0, 93.0, 95.0, 96.0, 98.0},
						}},
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
							time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
							94.1,
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
							time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
							94.0,
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
							time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
							92.0,
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
							time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
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
							time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
							6.0,
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
							time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
							2.0248456731316584,
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
							time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
							98.0,
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
							time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
							95.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "influxql.percentile",
			Args:   ", 90.0",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "percentile"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
							96.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "influxql.top",
			Args:   ", 2",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "top"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
							[]interface{}{98.0, 96.0},
						}},
					},
				},
			},
		},
		testCase{
			Method: "influxql.bottom",
			Args:   ", 3",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "bottom"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
							[]interface{}{92.0, 92.0, 92.0},
						}},
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
	}
}

func TestStream_CustomFunctions(t *testing.T) {
	t.Skip()
	var script = `
var fMap = loadMapFunc('./TestCustomMapFunction.py')
var fReduce = loadReduceFunc('./TestCustomReduceFunction.py')
stream
	.from('cpu')
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
	.from('cpu')
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
		expAns := `{"level":"CRITICAL","data":{"Series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:09Z",10]]}],"Err":null}}`
		if string(ans) != expAns {
			t.Errorf("got %v exp %v", string(ans), expAns)
		}
	}))
	defer ts.Close()

	var script = `
stream
	.from('cpu')
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
		expAns := `{"level":"INFO","data":{"Series":[{"name":"cpu","tags":{"host":"serverA","type":"idle"},"columns":["time","sigma","value"],"values":[["1970-01-01T00:00:07Z",2.469916402324427,16]]}],"Err":null}}`
		if string(ans) != expAns {
			t.Errorf("got %v exp %v", string(ans), expAns)
		}
	}))
	defer ts.Close()

	var script = `
stream
	.from('cpu')
	.where(lambda: "host" == 'serverA')
	.eval(lambda: sigma("value"))
		.as('sigma')
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
		expAns := `{"level":"CRITICAL","data":{"Series":[{"name":"cpu","tags":{"host":"serverA","type":"idle"},"columns":["time","value"],"values":[["1970-01-01T00:00:07Z",16]]}],"Err":null}}`
		if string(ans) != expAns {
			t.Errorf("unexpected result:\ngot %v\nexp %v", string(ans), expAns)
		}
	}))
	defer ts.Close()

	var script = `
stream
	.from('cpu')
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
	.from('cpu')
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
	.from('cpu')
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
		if len(p.Tags()) != 0 {
			t.Errorf("got %v exp %v", len(p.Tags()), 0)
		}
		tm := time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC)
		if !tm.Equal(p.Time()) {
			t.Errorf("times are not equal exp %s got %s", tm, p.Time())
		}
	}
}

// Helper test function for streamer
func testStreamer(t *testing.T, name, script string) (clock.Setter, *kapacitor.ExecutingTask, <-chan error, *kapacitor.TaskMaster) {
	if testing.Verbose() {
		wlog.LogLevel = wlog.DEBUG
	} else {
		wlog.LogLevel = wlog.OFF
	}

	//Create the task
	task, err := kapacitor.NewStreamer(name, script)
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
	c := clock.New(time.Unix(0, 0))
	r := kapacitor.NewReplay(c)

	// Create a new execution env
	tm := kapacitor.NewTaskMaster()
	tm.HTTPDService = httpService
	tm.Open()

	//Start the task
	et, err := tm.StartTask(task)
	if err != nil {
		t.Fatal(err)
	}

	// Replay test data to executor
	errCh := r.ReplayStream(data, tm.Stream)

	t.Log(string(et.Task.Dot()))
	return r.Setter, et, errCh, tm
}
