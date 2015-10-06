package integrations

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/services/httpd"
	"github.com/stretchr/testify/assert"
)

var httpService *httpd.Service

var NG = models.NilGroup

func init() {
	// create API server
	httpService = httpd.NewService(httpd.NewConfig())
	err := httpService.Open()
	if err != nil {
		panic(err)
	}
}

func nilGroup(pts []*models.Point) map[models.GroupID][]*models.Point {
	return map[models.GroupID][]*models.Point{
		NG: pts,
	}
}

func TestWindowDataByTime(t *testing.T) {
	assert := assert.New(t)

	var script = `
stream
	.from("cpu")
	.where("host = 'serverA'")
	.window()
		.period(10s)
		.every(10s)
	.httpOut("TestWindowDataByTime");
`

	tags := map[string]string{
		"type": "idle",
		"host": "serverA",
	}

	values := []float64{
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

	pts := make([]*models.Point, len(values))
	for i, v := range values {
		pts[i] = &models.Point{
			Name:   "cpu",
			Tags:   tags,
			Fields: map[string]interface{}{"value": v},
			Time:   time.Date(1970, 1, 1, 0, 0, i, 0, time.UTC),
		}
	}

	er := kapacitor.Result{Window: nilGroup(pts)}

	clock, et, errCh, tm := testStreamer(t, "TestWindowDataByTime", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(13 * time.Second))
	// Wait till the replay has finished
	assert.Nil(<-errCh)
	// Wait till the task is finished
	assert.Nil(et.Err())

	// Get the result
	output, err := et.GetOutput("TestWindowDataByTime")
	if !assert.Nil(err) {
		t.FailNow()
	}

	resp, err := http.Get(output.Endpoint())
	if !assert.Nil(err) {
		t.FailNow()
	}

	// Assert we got the expected result
	result := kapacitor.ResultFromJSON(resp.Body)
	if assert.Equal(len(er.Window), len(result.Window)) {
		for i := range er.Window[NG] {
			assert.Equal(er.Window[NG][i].Name, result.Window[NG][i].Name, "i: %d", i)
			assert.Equal(er.Window[NG][i].Tags, result.Window[NG][i].Tags, "i: %d", i)
			assert.Equal(er.Window[NG][i].Fields, result.Window[NG][i].Fields, "i: %d", i)
			assert.True(
				er.Window[NG][i].Time.Equal(result.Window[NG][i].Time),
				"i: %d %s != %s",
				i,
				er.Window[NG][i].Time, result.Window[NG][i].Time,
			)
		}
	}

}

func TestSimpleMapReduce(t *testing.T) {
	assert := assert.New(t)

	var script = `
stream
	.from("cpu")
	.where("host = 'serverA'")
	.window()
		.period(10s)
		.every(10s)
	.mapReduce(influxql.count, "value")
	.httpOut("TestSimpleMapReduce");
`
	er := kapacitor.Result{
		Window: nilGroup([]*models.Point{
			{
				Name:   "cpu",
				Fields: map[string]interface{}{"count": 10.0},
				Tags:   map[string]string{"type": "idle", "host": "serverA"},
				Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
			},
		}),
	}

	clock, et, errCh, tm := testStreamer(t, "TestSimpleMapReduce", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(15 * time.Second))
	// Wait till the replay has finished
	assert.Nil(<-errCh)
	// Wait till the task is finished
	assert.Nil(et.Err())

	// Get the result
	output, err := et.GetOutput("TestSimpleMapReduce")
	if !assert.Nil(err) {
		t.FailNow()
	}

	resp, err := http.Get(output.Endpoint())
	if !assert.Nil(err) {
		t.FailNow()
	}

	// Assert we got the expected result
	result := kapacitor.ResultFromJSON(resp.Body)
	if assert.Equal(len(er.Window), len(result.Window)) {
		for i := range er.Window[NG] {
			assert.Equal(er.Window[NG][i].Name, result.Window[NG][i].Name, "i: %d", i)
			assert.Equal(er.Window[NG][i].Tags, result.Window[NG][i].Tags, "i: %d", i)
			assert.Equal(er.Window[NG][i].Fields, result.Window[NG][i].Fields, "i: %d", i)
			assert.True(
				er.Window[NG][i].Time.Equal(result.Window[NG][i].Time),
				"i: %d %s != %s",
				i,
				er.Window[NG][i].Time, result.Window[NG][i].Time,
			)
		}
	}
}

func TestSplitStreamData(t *testing.T) {

	var script = `
var cpu = stream
	.from("cpu")
	.where("dc = 'nyc'");

cpu
	.where("host = 'serverA'");
	.window()
		.period(1s)
		.every(1s)
	.cache("/a");

cpu
	.where("host = 'serverB'");
	.window()
		.period(1s)
		.every(1s)
	.cache("/b");
`
	//er := kapacitor.Result{}

	testStreamer(t, "TestSplitStreamData", script)
}
func TestGroupByStream(t *testing.T) {
	assert := assert.New(t)

	var script = `
stream
	.from("errors")
	.groupBy("service")
	.window()
		.period(10s)
		.every(10s)
	.mapReduce(influxql.sum, "value")
	.httpOut("error_count");
`

	er := kapacitor.Result{
		Window: map[models.GroupID][]*models.Point{
			"service=cartA,": {
				{
					Name:   "errors",
					Tags:   map[string]string{"service": "cartA", "dc": "A"},
					Fields: map[string]interface{}{"sum": 47.0},
					Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
				},
			},
			"service=login,": {
				{
					Name:   "errors",
					Tags:   map[string]string{"service": "login", "dc": "B"},
					Fields: map[string]interface{}{"sum": 45.0},
					Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
				},
			},
			"service=front,": {
				{
					Name:   "errors",
					Tags:   map[string]string{"service": "front", "dc": "B"},
					Fields: map[string]interface{}{"sum": 32.0},
					Time:   time.Date(1970, 1, 1, 0, 0, 8, 0, time.UTC),
				},
			},
		},
	}

	clock, et, errCh, tm := testStreamer(t, "TestGroupByStream", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(13 * time.Second))
	// Wait till the replay has finished
	assert.Nil(<-errCh)
	// Wait till the task is finished
	assert.Nil(et.Err())

	// Get the result
	output, err := et.GetOutput("error_count")
	if !assert.Nil(err) {
		t.FailNow()
	}

	resp, err := http.Get(output.Endpoint())
	if !assert.Nil(err) {
		t.FailNow()
	}

	// Assert we got the expected result
	result := kapacitor.ResultFromJSON(resp.Body)
	if assert.Equal(len(er.Window), len(result.Window)) {
		for g := range er.Window {
			if assert.Equal(len(er.Window[g]), len(result.Window[g])) {
				for i := range er.Window[g] {
					assert.Equal(er.Window[g][i].Name, result.Window[g][i].Name, "g: %s i: %d", g, i)
					assert.Equal(er.Window[g][i].Tags, result.Window[g][i].Tags, "g: %s i: %d", g, i)
					assert.Equal(er.Window[g][i].Fields, result.Window[g][i].Fields, "g: %s i: %d", g, i)
					assert.True(
						er.Window[g][i].Time.Equal(result.Window[g][i].Time),
						"g: %s i: %d %s != %s",
						g,
						i,
						er.Window[g][i].Time, result.Window[g][i].Time,
					)
				}
			}
		}
	}
}

func TestJoinStreamData(t *testing.T) {
	assert := assert.New(t)

	var script = `
var errorCounts = stream
			.fork()
			.from("errors")
			.groupBy("service")
			.window()
				.period(10s)
				.every(10s)
			.mapReduce(influxql.sum, "value");

var viewCounts = stream
			.fork()
			.from("views")
			.groupBy("service")
			.window()
				.period(10s)
				.every(10s)
			.mapReduce(influxql.sum, "value");

errorCounts.join(viewCounts)
		.as("errors", "views")
		.rename("error_rate")
		.apply(expr("value", "errors.sum / views.sum"))
		.httpOut("error_rate");
`

	er := kapacitor.Result{
		Window: map[models.GroupID][]*models.Point{
			"service=cartA,": {
				{
					Name:   "error_rate",
					Tags:   map[string]string{"service": "cartA", "dc": "A"},
					Fields: map[string]interface{}{"value": 0.01},
					Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
				},
			},
			"service=login,": {
				{
					Name:   "error_rate",
					Tags:   map[string]string{"service": "login", "dc": "B"},
					Fields: map[string]interface{}{"value": 0.01},
					Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
				},
			},
			"service=front,": {
				{
					Name:   "error_rate",
					Tags:   map[string]string{"service": "front", "dc": "B"},
					Fields: map[string]interface{}{"value": 0.01},
					Time:   time.Date(1970, 1, 1, 0, 0, 8, 0, time.UTC),
				},
			},
		},
	}

	clock, et, errCh, tm := testStreamer(t, "TestJoinStreamData", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(13 * time.Second))
	// Wait till the replay has finished
	assert.Nil(<-errCh)
	// Wait till the task is finished
	assert.Nil(et.Err())

	// Get the result
	output, err := et.GetOutput("error_rate")
	if !assert.Nil(err) {
		t.FailNow()
	}

	resp, err := http.Get(output.Endpoint())
	if !assert.Nil(err) {
		t.FailNow()
	}

	// Assert we got the expected result
	result := kapacitor.ResultFromJSON(resp.Body)
	if assert.Equal(len(er.Window), len(result.Window)) {
		for g := range er.Window {
			if assert.Equal(len(er.Window[g]), len(result.Window[g])) {
				for i := range er.Window[g] {
					assert.Equal(er.Window[g][i].Name, result.Window[g][i].Name, "g: %s i: %d", g, i)
					assert.Equal(er.Window[g][i].Tags, result.Window[g][i].Tags, "g: %s i: %d", g, i)
					assert.Equal(er.Window[g][i].Fields, result.Window[g][i].Fields, "g: %s i: %d", g, i)
					assert.True(
						er.Window[g][i].Time.Equal(result.Window[g][i].Time),
						"g: %s i: %d %s != %s",
						g,
						i,
						er.Window[g][i].Time, result.Window[g][i].Time,
					)
				}
			}
		}
	}

}

func TestUnionStreamData(t *testing.T) {
	assert := assert.New(t)

	var script = `
var cpu = stream
			.fork()
				.from("cpu")
				.where("cpu = 'total'");
var mem = stream
			.fork()
				.from("memory")
				.where("type = 'free'");
var disk = stream
			.fork()
				.from("disk")
				.where("device = 'sda'");

cpu.union(mem, disk)
		.window()
			.period(10s)
			.every(10s)
		.mapReduce(influxql.count, "value")
		.httpOut("all");
`

	er := kapacitor.Result{
		Window: nilGroup([]*models.Point{
			{
				Name:   "disk",
				Tags:   map[string]string{"device": "sda", "host": "serverB"},
				Fields: map[string]interface{}{"count": 24.0},
				Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
			},
		}),
	}

	clock, et, errCh, tm := testStreamer(t, "TestUnionStreamData", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(15 * time.Second))
	// Wait till the replay has finished
	assert.Nil(<-errCh)
	// Wait till the task is finished
	assert.Nil(et.Err())

	// Get the result
	output, err := et.GetOutput("all")
	if !assert.Nil(err) {
		t.FailNow()
	}

	resp, err := http.Get(output.Endpoint())
	if !assert.Nil(err) {
		t.FailNow()
	}

	// Assert we got the expected result
	result := kapacitor.ResultFromJSON(resp.Body)
	if assert.Equal(len(er.Window), len(result.Window)) {
		for i := range er.Window[NG] {
			assert.Equal(er.Window[NG][i].Name, result.Window[NG][i].Name, "i: %d", i)
			assert.Equal(er.Window[NG][i].Tags, result.Window[NG][i].Tags, "i: %d", i)
			assert.Equal(er.Window[NG][i].Fields, result.Window[NG][i].Fields, "i: %d", i)
			assert.True(
				er.Window[NG][i].Time.Equal(result.Window[NG][i].Time),
				"i: %d %s != %s",
				i,
				er.Window[NG][i].Time, result.Window[NG][i].Time,
			)
		}
	}
}

func TestStreamAggregations(t *testing.T) {
	assert := assert.New(t)

	type testCase struct {
		Method string
		ER     kapacitor.Result
	}

	var scriptTmpl = `
stream
	.from("cpu")
	.where("host = 'serverA'")
	.window()
		.period(10s)
		.every(10s)
	.mapReduce({{ .Method }}, "value")
	.httpOut("{{ .Method }}");
`
	tags := map[string]string{
		"type": "idle",
		"host": "serverA",
	}

	testCases := []testCase{
		testCase{
			Method: "influxql.sum",
			ER: kapacitor.Result{
				Window: nilGroup([]*models.Point{
					{
						Name:   "cpu",
						Tags:   tags,
						Fields: map[string]interface{}{"sum": 941.0},
						Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
					},
				}),
			},
		},
		testCase{
			Method: "influxql.count",
			ER: kapacitor.Result{
				Window: nilGroup([]*models.Point{
					{
						Name:   "cpu",
						Tags:   tags,
						Fields: map[string]interface{}{"count": 10.0},
						Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
					},
				}),
			},
		},
		testCase{
			Method: "influxql.distinct",
			ER: kapacitor.Result{
				Window: nilGroup([]*models.Point{
					{
						Name:   "cpu",
						Tags:   tags,
						Fields: map[string]interface{}{"distinct": []interface{}{92.0, 93.0, 95.0, 96.0, 98.0}},
						Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
					},
				}),
			},
		},
		testCase{
			Method: "influxql.mean",
			ER: kapacitor.Result{
				Window: nilGroup([]*models.Point{
					{
						Name:   "cpu",
						Tags:   tags,
						Fields: map[string]interface{}{"mean": 94.1},
						Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
					},
				}),
			},
		},
		testCase{
			Method: "influxql.median",
			ER: kapacitor.Result{
				Window: nilGroup([]*models.Point{
					{
						Name:   "cpu",
						Tags:   tags,
						Fields: map[string]interface{}{"median": 94.0},
						Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
					},
				}),
			},
		},
		testCase{
			Method: "influxql.min",
			ER: kapacitor.Result{
				Window: nilGroup([]*models.Point{
					{
						Name:   "cpu",
						Tags:   tags,
						Fields: map[string]interface{}{"min": 92.0},
						Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
					},
				}),
			},
		},
		testCase{
			Method: "influxql.max",
			ER: kapacitor.Result{
				Window: nilGroup([]*models.Point{
					{
						Name:   "cpu",
						Tags:   tags,
						Fields: map[string]interface{}{"max": 98.0},
						Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
					},
				}),
			},
		},
		testCase{
			Method: "influxql.spread",
			ER: kapacitor.Result{
				Window: nilGroup([]*models.Point{
					{
						Name:   "cpu",
						Tags:   tags,
						Fields: map[string]interface{}{"spread": 6.0},
						Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
					},
				}),
			},
		},
		testCase{
			Method: "influxql.stddev",
			ER: kapacitor.Result{
				Window: nilGroup([]*models.Point{
					{
						Name:   "cpu",
						Tags:   tags,
						Fields: map[string]interface{}{"stddev": 2.0248456731316584},
						Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
					},
				}),
			},
		},
		testCase{
			Method: "influxql.first",
			ER: kapacitor.Result{
				Window: nilGroup([]*models.Point{
					{
						Name:   "cpu",
						Tags:   tags,
						Fields: map[string]interface{}{"first": 98.0},
						Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
					},
				}),
			},
		},
		testCase{
			Method: "influxql.last",
			ER: kapacitor.Result{
				Window: nilGroup([]*models.Point{
					{
						Name:   "cpu",
						Tags:   tags,
						Fields: map[string]interface{}{"last": 95.0},
						Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
					},
				}),
			},
		},
	}

	tmpl, err := template.New("script").Parse(scriptTmpl)
	if !assert.Nil(err) {
		t.FailNow()
	}

	for _, tc := range testCases {
		log.Println("Method:", tc.Method)
		var script bytes.Buffer
		tmpl.Execute(&script, tc)
		clock, et, errCh, tm := testStreamer(
			t,
			"TestStreamAggregation",
			string(script.Bytes()),
		)
		defer tm.Close()

		// Move time forward
		clock.Set(clock.Zero().Add(13 * time.Second))
		// Wait till the replay has finished
		assert.Nil(<-errCh)
		// Wait till the task is finished
		assert.Nil(et.Err())

		// Get the result
		output, err := et.GetOutput(tc.Method)
		if !assert.Nil(err) {
			t.FailNow()
		}

		resp, err := http.Get(output.Endpoint())
		if !assert.Nil(err) {
			t.FailNow()
		}

		// Assert we got the expected result
		result := kapacitor.ResultFromJSON(resp.Body)
		if assert.Equal(len(tc.ER.Window), len(result.Window)) {
			for i := range tc.ER.Window[NG] {
				assert.Equal(tc.ER.Window[NG][i].Name, result.Window[NG][i].Name, "i: %d", i)
				assert.Equal(tc.ER.Window[NG][i].Tags, result.Window[NG][i].Tags, "i: %d", i)
				assert.Equal(tc.ER.Window[NG][i].Fields, result.Window[NG][i].Fields, "i: %d", i)
				assert.True(
					tc.ER.Window[NG][i].Time.Equal(result.Window[NG][i].Time),
					"i: %d %s != %s",
					i,
					tc.ER.Window[NG][i].Time, result.Window[NG][i].Time,
				)
			}
		}
	}
}

func TestCustomFunctions(t *testing.T) {
	var script = `
var fMap = loadMapFunc("./TestCustomMapFunction.py");
var fReduce = loadReduceFunc("./TestCustomReduceFunction.py");
stream
	.from("cpu")
	.where("host = 'serverA'")
	.window()
		.period(1s)
		.every(1s)
	.map(fMap, "idle")
	.reduce(fReduce)
	.cache();
`

	//er := kapacitor.Result{}

	testStreamer(t, "TestCustomFunctions", script)
}

func TestCustomMRFunction(t *testing.T) {
	var script = `
var fMapReduce = loadMapReduceFunc("./TestCustomMapReduceFunction.py");
stream
	.from("cpu")
	.where("host = 'serverA'")
	.window()
		.period(1s)
		.every(1s)
	.mapReduce(fMap, "idle")
	.cache();
`

	//er := kapacitor.Result{}

	testStreamer(t, "TestCustomMRFunction", script)
}

func TestStreamingAlert(t *testing.T) {
	assert := assert.New(t)

	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ans, err := ioutil.ReadAll(r.Body)
		if !assert.Nil(err) {
			t.FailNow()
		}
		requestCount++
		expAns := `[{"Name":"cpu","Group":"","Tags":{"host":"serverA","type":"idle"},"Fields":{"count":10},"Time":"1970-01-01T00:00:09Z"}]`
		assert.Equal(expAns, string(ans))
	}))
	defer ts.Close()

	var script = `
stream
	.from("cpu")
	.where("host = 'serverA'")
	.window()
		.period(10s)
		.every(10s)
	.mapReduce(influxql.count, "idle")
	.alert()
		.predicate("count > 5")
		.post("` + ts.URL + `");`

	clock, et, errCh, tm := testStreamer(t, "TestStreamingAlert", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(13 * time.Second))
	// Wait till the replay has finished
	assert.Nil(<-errCh)
	// Wait till the task is finished
	assert.Nil(et.Err())

	assert.Equal(1, requestCount)
}

func TestStreamingAlertSigma(t *testing.T) {
	assert := assert.New(t)

	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ans, err := ioutil.ReadAll(r.Body)
		if !assert.Nil(err) {
			t.FailNow()
		}
		requestCount++
		expAns := `[{"Name":"cpu","Group":"","Tags":{"host":"serverA","type":"idle"},"Fields":{"value":16},"Time":"1970-01-01T00:00:07Z"}]`
		assert.Equal(expAns, string(ans))
	}))
	defer ts.Close()

	var script = `
stream
	.from("cpu")
	.where("host = 'serverA'")
	.alert()
		.predicate("sigma(value) > 2")
		.post("` + ts.URL + `");`

	clock, et, errCh, tm := testStreamer(t, "TestStreamingAlertSigma", script)
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(13 * time.Second))
	// Wait till the replay has finished
	assert.Nil(<-errCh)
	// Wait till the task is finished
	assert.Nil(et.Err())

	assert.Equal(1, requestCount)
}

func TestStreamingAlertFlapping(t *testing.T) {
	var script = `
stream
	.from("cpu")
	.where("host = 'serverA'")
	.window()
		.period(1s)
		.every(1s)
	.mapReduce(influxql.percentile(10), "idle")
	.where("percentile < 30")
	.alert()
		.flapping(25, 50)
		.post("http://localhost");
`

	//er := kapacitor.Result{}

	testStreamer(t, "TestStreamingAlertFlapping", script)
}
func TestInfluxDBOut(t *testing.T) {
	assert := assert.New(t)

	var script = `
stream
	.from("cpu")
	.where("host = 'serverA'")
	.window()
		.period(10s)
		.every(10s)
	.mapReduce(influxql.count, "value")
	.influxDBOut()
		.database("db")
		.retentionPolicy("rp")
		.measurement("m");
`
	//er := kapacitor.Result{
	//	Window: nilGroup([]*models.Point{
	//		{
	//			Name:   "cpu",
	//			Fields: map[string]interface{}{"count": 10.0},
	//			Tags:   map[string]string{"type": "idle", "host": "serverA"},
	//			Time:   time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC),
	//		},
	//	}),
	//}

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
		log.Println(r.URL)
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

	clock, et, errCh, tm := testStreamer(t, "TestInfluxDBOut", script)
	tm.InfluxDBService = influxdb
	defer tm.Close()

	// Move time forward
	clock.Set(clock.Zero().Add(15 * time.Second))
	// Wait till the replay has finished
	assert.Nil(<-errCh)
	// Wait till the task is finished
	assert.Nil(et.Err())
	// Wait till we received a request
	assert.Nil(<-done)

	assert.Equal("db", database)
	assert.Equal("rp", rp)
	assert.Equal("s", precision, "This fix is waiting on https://github.com/influxdb/influxdb/issues/4344")
	log.Println(points)
	if assert.Equal(1, len(points)) {
		p := points[0]
		assert.Equal("m", p.Name())
		assert.Equal(imodels.Fields(map[string]interface{}{"count": 10.0}), p.Fields())
		assert.Equal(imodels.Tags(map[string]string{"type": "idle", "host": "serverA"}), p.Tags())
		assert.True(time.Date(1970, 1, 1, 0, 0, 9, 0, time.UTC).Equal(p.Time()))
	}
}

// Helper test function for streamer
func testStreamer(t *testing.T, name, script string) (clock.Setter, *kapacitor.ExecutingTask, <-chan error, *kapacitor.TaskMaster) {
	assert := assert.New(t)

	//Create the task
	task, err := kapacitor.NewStreamer(name, script)
	if !assert.Nil(err) {
		t.FailNow()
	}

	// Load test data
	dir, err := os.Getwd()
	if !assert.Nil(err) {
		t.FailNow()
	}
	data, err := os.Open(path.Join(dir, "data", name+".rpl"))
	if !assert.Nil(err) {
		t.FailNow()
	}
	c := clock.New(time.Unix(0, 0))
	r := kapacitor.NewReplay(c)

	// Create a new execution env
	tm := kapacitor.NewTaskMaster()
	tm.HTTPDService = httpService
	tm.Open()

	//Start the task
	et, err := tm.StartTask(task)
	if !assert.Nil(err) {
		t.FailNow()
	}

	// Replay test data to executor
	errCh := r.ReplayStream(data, tm.Stream)

	fmt.Fprintln(os.Stderr, string(et.Task.Dot()))
	return r.Setter, et, errCh, tm
}
