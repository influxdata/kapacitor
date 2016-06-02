package integrations

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"reflect"
	"sync/atomic"
	"testing"
	"text/template"
	"time"

	"github.com/influxdata/influxdb/client"
	"github.com/influxdata/influxdb/influxql"
	imodels "github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/clock"
	"github.com/influxdata/kapacitor/services/alerta"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/sensu"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/talk"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/influxdata/kapacitor/udf"
	"github.com/influxdata/kapacitor/udf/test"
	"github.com/influxdata/wlog"
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

func TestStream_Derivative(t *testing.T) {

	var script = `
stream
	|from().measurement('packets')
	|derivative('value')
	|window()
		.period(10s)
		.every(10s)
	|mean('value')
	|httpOut('TestStream_Derivative')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					1.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Derivative", script, 15*time.Second, er, nil, false)
}

func TestStream_DerivativeZeroElapsed(t *testing.T) {

	var script = `
stream
	|from().measurement('packets')
	|derivative('value')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|httpOut('TestStream_DerivativeZeroElapsed')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					9.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_DerivativeZeroElapsed", script, 15*time.Second, er, nil, false)
}

func TestStream_DerivativeUnit(t *testing.T) {

	var script = `
stream
	|from().measurement('packets')
	|derivative('value')
		.unit(10s)
	|window()
		.period(10s)
		.every(10s)
	|mean('value')
	|httpOut('TestStream_Derivative')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Derivative", script, 15*time.Second, er, nil, false)
}

func TestStream_DerivativeNN(t *testing.T) {

	var script = `
stream
	|from().measurement('packets')
	|derivative('value')
		.nonNegative()
	|window()
		.period(10s)
		.every(10s)
	|mean('value')
	|httpOut('TestStream_DerivativeNN')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					1.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_DerivativeNN", script, 15*time.Second, er, nil, false)
}

func TestStream_DerivativeN(t *testing.T) {

	var script = `
stream
	|from().measurement('packets')
	|derivative('value')
	|window()
		.period(10s)
		.every(10s)
	|mean('value')
	|httpOut('TestStream_DerivativeNN')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					-99.7,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_DerivativeNN", script, 15*time.Second, er, nil, false)
}

func TestStream_Elapsed(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('packets')
	|elapsed('value', 1s)
	|window()
		.period(10s)
		.every(10s)
	|max('elapsed')
	|httpOut('TestStream_Elapsed')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
					4.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Elapsed", script, 15*time.Second, er, nil, false)
}

func TestStream_WindowMissing(t *testing.T) {

	var script = `
var period = 3s
var every = 2s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(period)
		.every(every)
	|count('value')
	|httpOut('TestStream_WindowMissing')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
					3.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_WindowMissing", script, 13*time.Second, er, nil, false)
}

func TestStream_WindowMissingAligned(t *testing.T) {

	var script = `
var period = 3s
var every = 2s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(period)
		.every(every)
		.align()
	|count('value')
	|httpOut('TestStream_WindowMissing')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					3.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_WindowMissing", script, 13*time.Second, er, nil, false)
}

func TestStream_Window(t *testing.T) {

	var script = `
var period = 10s
var every = 10s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(period)
		.every(every)
	|httpOut('TestStream_Window')
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

	testStreamerWithOutput(t, "TestStream_Window", script, 13*time.Second, er, nil, false)
}

func TestStream_Shift(t *testing.T) {

	var script = `
var period  = 5s

var data  = stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')

var past = data
	|window()
		.period(period)
		.every(period)
		.align()
	|count('value')
	|shift(period)

var current = data
	|window()
		.period(period)
		.every(period)
		.align()
	|count('value')

past
	|join(current)
		.as('past', 'current')
	|eval(lambda: "current.count" - "past.count")
		.keep()
		.as('diff')
	|httpOut('TestStream_Shift')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "current.count", "diff", "past.count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					5.0,
					1.0,
					4.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Shift", script, 15*time.Second, er, nil, false)
}

func TestStream_ShiftBatch(t *testing.T) {

	var script = `
var period  = 5s

var data  = stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')

var past = data
	|window()
		.period(period)
		.every(period)
		.align()
	|shift(period)
	|count('value')

var current = data
	|window()
		.period(period)
		.every(period)
		.align()
	|count('value')

past
	|join(current)
		.as('past', 'current')
	|eval(lambda: "current.count" - "past.count")
		.keep()
		.as('diff')
	|httpOut('TestStream_Shift')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "current.count", "diff", "past.count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					5.0,
					1.0,
					4.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Shift", script, 15*time.Second, er, nil, false)
}

func TestStream_ShiftNegative(t *testing.T) {

	var script = `
var period  = 5s

var data  = stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')

var past = data
	|window()
		.period(period)
		.every(period)
		.align()
	|count('value')

var current = data
	|window()
		.period(period)
		.every(period)
		.align()
	|count('value')
	|shift(-period)

past
	|join(current)
		.as('past', 'current')
	|eval(lambda: "current.count" - "past.count")
		.keep()
		.as('diff')
	|httpOut('TestStream_Shift')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "current.count", "diff", "past.count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					5.0,
					1.0,
					4.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Shift", script, 15*time.Second, er, nil, false)
}

func TestStream_ShiftBatchNegative(t *testing.T) {

	var script = `
var period  = 5s

var data  = stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')

var past = data
	|window()
		.period(period)
		.every(period)
		.align()
	|count('value')

var current = data
	|window()
		.period(period)
		.every(period)
		.align()
	|shift(-period)
	|count('value')

past
	|join(current)
		.as('past', 'current')
	|eval(lambda: "current.count" - "past.count")
		.keep()
		.as('diff')
	|httpOut('TestStream_Shift')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "current.count", "diff", "past.count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					5.0,
					1.0,
					4.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Shift", script, 15*time.Second, er, nil, false)
}

func TestStream_SimpleMR(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|httpOut('TestStream_SimpleMR')
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

	testStreamerWithOutput(t, "TestStream_SimpleMR", script, 15*time.Second, er, nil, false)
}

func TestStream_EvalAllTypes(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
	|eval(lambda: "str" + 'suffix', lambda: !"bool", lambda: "int" + 14, lambda: "float" * 2.0)
		.as( 'str', 'bool', 'int', 'float')
	|httpOut('TestStream_EvalAllTypes')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "types",
				Tags:    nil,
				Columns: []string{"time", "bool", "float", "int", "str"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					true,
					84.0,
					19.0,
					"bobsuffix",
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_EvalAllTypes", script, 2*time.Second, er, nil, false)
}

func TestStream_EvalGroups(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
		.groupBy('group')
	|eval(lambda: count())
		.as('count')
	|httpOut('TestStream_EvalGroups')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "types",
				Tags:    map[string]string{"group": "A"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						2.0,
					},
				},
			},
			{
				Name:    "types",
				Tags:    map[string]string{"group": "B"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						2.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_EvalGroups", script, 3*time.Second, er, nil, false)
}

func TestStream_Eval_Time(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
		.groupBy('group')
	|log()
	|eval(lambda: hour("time"))
		.as('hour')
	|httpOut('TestStream_Eval_Time')
`
	hour := float64(time.Date(1971, 1, 1, 1, 0, 0, 0, time.UTC).Local().Hour())
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "types",
				Tags:    map[string]string{"group": "A"},
				Columns: []string{"time", "hour"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 1, 0, 0, 0, time.UTC),
						hour,
					},
				},
			},
			{
				Name:    "types",
				Tags:    map[string]string{"group": "B"},
				Columns: []string{"time", "hour"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 1, 0, 0, 0, time.UTC),
						hour,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Eval_Time", script, 2*time.Hour, er, nil, false)
}

func TestStream_Default(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('cpu')
	|default()
		.field('value', 1.0)
		.tag('host', 'serverA')
	|where(lambda: "host" == 'serverA')
	|window()
		.period(10s)
		.every(10s)
	|sum('value')
	|httpOut('TestStream_Default')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					57.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Default", script, 15*time.Second, er, nil, false)
}

func TestStream_AllMeasurements(t *testing.T) {

	var script = `
stream
	|from()
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|httpOut('TestStream_AllMeasurements')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					23.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_AllMeasurements", script, 15*time.Second, er, nil, false)
}

func TestStream_HttpOutPassThrough(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|httpOut('unused')
	|httpOut('TestStream_SimpleMR')
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

	testStreamerWithOutput(t, "TestStream_SimpleMR", script, 15*time.Second, er, nil, false)
}

func TestStream_BatchGroupBy(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
	|window()
		.period(5s)
		.every(5s)
	|groupBy('host')
	|count('value')
	|httpOut('TestStream_BatchGroupBy')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					5.0,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverB"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					5.0,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverC"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					1.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_BatchGroupBy", script, 15*time.Second, er, nil, true)
}

func TestStream_BatchGroupByAll(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
	|window()
		.period(5s)
		.every(5s)
	|groupBy(*)
	|count('value')
	|httpOut('TestStream_BatchGroupBy')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA", "type": "idle"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					5.0,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverB", "type": "idle"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					5.0,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverC", "type": "idle"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					1.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_BatchGroupBy", script, 15*time.Second, er, nil, true)
}

func TestStream_SimpleWhere(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.where(lambda: "host" != 'serverB')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|where(lambda: "count" > 0)
	|where(lambda: "count" < 12)
	|httpOut('TestStream_SimpleMR')
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

	testStreamerWithOutput(t, "TestStream_SimpleMR", script, 15*time.Second, er, nil, false)
}

func TestStream_VarWhereString(t *testing.T) {

	var script = `
var serverStr = 'serverA'
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == serverStr )
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|httpOut('TestStream_SimpleMR')
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

	testStreamerWithOutput(t, "TestStream_SimpleMR", script, 15*time.Second, er, nil, false)
}

func TestStream_VarWhereRegex(t *testing.T) {

	var script = `
var serverPattern = /^serverA$/
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" =~ serverPattern )
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|httpOut('TestStream_SimpleMR')
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

	testStreamerWithOutput(t, "TestStream_SimpleMR", script, 15*time.Second, er, nil, false)
}

func TestStream_GroupBy(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('errors')
		.groupBy('service')
	|window()
		.period(10s)
		.every(10s)
	|sum('value')
	|httpOut('TestStream_GroupBy')
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

	testStreamerWithOutput(t, "TestStream_GroupBy", script, 13*time.Second, er, nil, false)
}

func TestStream_GroupByWhere(t *testing.T) {

	var script = `
var serverA = stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')

var byCpu = serverA
	|groupBy('host', 'cpu')

var total = serverA
	|where(lambda: "cpu" == 'cpu-total')

byCpu
	|join(total)
		.on('host')
		.as('cpu', 'total')
	|eval(lambda: "cpu.value" / "total.value")
		.as('cpu_percent')
	|window()
		.period(10s)
		.every(10s)
	|mean('cpu_percent')
	|httpOut('TestStream_GroupByWhere')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"cpu": "cpu0", "host": "serverA"},
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.7823116704593873,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"cpu": "cpu1", "host": "serverA"},
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.7676074281820646,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"cpu": "cpu-total", "host": "serverA"},
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					1.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_GroupByWhere", script, 13*time.Second, er, nil, true)
}

func TestStream_Join(t *testing.T) {

	var script = `
var errorCounts = stream
	|from()
		.measurement('errors')
		.groupBy('service')
	|window()
		.period(10s)
		.every(10s)
		.align()
	|sum('value')

var viewCounts = stream
	|from()
		.measurement('views')
		.groupBy('service')
	|window()
		.period(10s)
		.every(10s)
		.align()
	|sum('value')

errorCounts
	|join(viewCounts)
		.as('errors', 'views')
		.streamName('error_view')
	|eval(lambda: "errors.sum" / "views.sum")
		.as('error_percent')
		.keep()
	|httpOut('TestStream_Join')
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

	testStreamerWithOutput(t, "TestStream_Join", script, 13*time.Second, er, nil, true)
}

func TestStream_JoinTolerance(t *testing.T) {

	var script = `
var errorCounts = stream
	|from()
		.measurement('errors')
		.groupBy('service')

var viewCounts = stream
	|from()
		.measurement('views')
		.groupBy('service')

errorCounts
	|join(viewCounts)
		.as('errors', 'views')
		.tolerance(2s)
		.streamName('error_view')
	|window()
		.period(10s)
		.every(10s)
	|httpOut('TestStream_JoinTolerance')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "cartA"},
				Columns: []string{"time", "errors.value", "views.value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						7.0,
						700.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						9.0,
						900.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						3.0,
						300.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						11.0,
						1100.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						12.0,
						1200.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						6.0,
						600.0,
					},
				},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "login"},
				Columns: []string{"time", "errors.value", "views.value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						9.0,
						900.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						5.0,
						500.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						9.0,
						900.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						2.0,
						200.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						7.0,
						700.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						10.0,
						1000.0,
					},
				},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "front"},
				Columns: []string{"time", "errors.value", "views.value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						2.0,
						200.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						9.0,
						900.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						7.0,
						700.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						5.0,
						500.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						4.0,
						400.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						6.0,
						600.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						4.0,
						400.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_JoinTolerance", script, 13*time.Second, er, nil, true)
}

func TestStream_JoinFill(t *testing.T) {
	var script = `
var errorCounts = stream
	|from()
		.measurement('errors')
		.groupBy('service')

var viewCounts = stream
	|from()
		.measurement('views')
		.groupBy('service')

errorCounts
	|join(viewCounts)
		.as('errors', 'views')
		.fill(0.0)
		.streamName('error_view')
	|eval(lambda:  "errors.value" + "views.value")
		.as('error_percent')
	|window()
		.period(10s)
		.every(10s)
	|count('error_percent')
	|httpOut('TestStream_JoinFill')
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

	testStreamerWithOutput(t, "TestStream_JoinFill", script, 13*time.Second, er, nil, true)
}

func TestStream_JoinN(t *testing.T) {

	var script = `
var cpu = stream
	|from()
		.measurement('cpu')
		.where(lambda: "cpu" == 'total')
var mem = stream
	|from()
		.measurement('memory')
		.where(lambda: "type" == 'free')
var disk = stream
	|from()
		.measurement('disk')
		.where(lambda: "device" == 'sda')

cpu
	|join(mem, disk)
		.as('cpu', 'mem', 'disk')
		.streamName('magic')
		.fill(0.0)
	|window()
		.period(10s)
		.every(10s)
	|count('cpu.value')
	|httpOut('TestStream_JoinN')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "magic",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_JoinN", script, 15*time.Second, er, nil, false)
}

func TestStream_JoinOn(t *testing.T) {
	var script = `
var errorsByServiceDC = stream
	|from()
		.measurement('errors')
		.groupBy('dc', 'service')
	|window()
		.period(10s)
		.every(10s)
		.align()
	|sum('value')

var errorsByServiceGlobal = stream
	|from()
		.measurement('errors')
		.groupBy('service')
	|window()
		.period(10s)
		.every(10s)
		.align()
	|sum('value')

errorsByServiceGlobal
	|join(errorsByServiceDC)
		.as('service', 'dc')
		.on('service')
		.streamName('dc_error_percent')
	|eval(lambda: "dc.sum" / "service.sum")
		.keep()
		.as('value')
	|httpOut('TestStream_JoinOn')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "cartA"},
				Columns: []string{"time", "dc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					15.0,
					47.0,
					15.0 / 47.0,
				}},
			},
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "cartA"},
				Columns: []string{"time", "dc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					32.0,
					47.0,
					32.0 / 47.0,
				}},
			},
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "login"},
				Columns: []string{"time", "dc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					15.0,
					45.0,
					15.0 / 45.0,
				}},
			},
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "login"},
				Columns: []string{"time", "dc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					23.0,
					45.0,
					23.0 / 45.0,
				}},
			},
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "C", "service": "login"},
				Columns: []string{"time", "dc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					7.0,
					45.0,
					7.0 / 45.0,
				}},
			},
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "front"},
				Columns: []string{"time", "dc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					19.0,
					32.0,
					19.0 / 32.0,
				}},
			},
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "front"},
				Columns: []string{"time", "dc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					13.0,
					32.0,
					13.0 / 32.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_JoinOn", script, 13*time.Second, er, nil, true)
}

func TestStream_JoinOnGap(t *testing.T) {
	var script = `
var errorsByServiceDCRack = stream
	|from()
		.measurement('errors')
		.groupBy('dc', 'service', 'rack')
	|window()
		.period(10s)
		.every(10s)
		.align()
	|sum('value')

var errorsByServiceGlobal = stream
	|from()
		.measurement('errors')
		.groupBy('service')
	|window()
		.period(10s)
		.every(10s)
		.align()
	|sum('value')

errorsByServiceGlobal
	|join(errorsByServiceDCRack)
		.as('service', 'loc')
		.on('service')
		.streamName('loc_error_percent')
	|eval(lambda: "loc.sum" / "service.sum")
		.keep()
		.as('value')
	|httpOut('TestStream_JoinOn')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "cartA", "rack": "0"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
					47.0,
					10.0 / 47.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "cartA", "rack": "1"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					5.0,
					47.0,
					5.0 / 47.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "cartA", "rack": "0"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					14.0,
					47.0,
					14.0 / 47.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "cartA", "rack": "1"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					18.0,
					47.0,
					18.0 / 47.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "login", "rack": "0"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					5.0,
					45.0,
					5.0 / 45.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "login", "rack": "1"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
					45.0,
					10.0 / 45.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "login", "rack": "0"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					3.0,
					45.0,
					3.0 / 45.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "login", "rack": "1"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					20.0,
					45.0,
					20.0 / 45.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "C", "service": "login", "rack": "0"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					7.0,
					45.0,
					7.0 / 45.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "front", "rack": "0"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					9.0,
					32.0,
					9.0 / 32.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "front", "rack": "1"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
					32.0,
					10.0 / 32.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "front", "rack": "0"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					4.0,
					32.0,
					4.0 / 32.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "front", "rack": "1"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					9.0,
					32.0,
					9.0 / 32.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_JoinOn", script, 13*time.Second, er, nil, true)
}

func TestStream_Union(t *testing.T) {

	var script = `
var cpu = stream
	|from()
		.measurement('cpu')
		.where(lambda: "cpu" == 'total')
var mem = stream
	|from()
		.measurement('memory')
		.where(lambda: "type" == 'free')
var disk = stream
	|from()
		.measurement('disk')
		.where(lambda: "device" == 'sda')

cpu
	|union(mem, disk)
		.rename('cpu_mem_disk')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|httpOut('TestStream_Union')
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

	testStreamerWithOutput(t, "TestStream_Union", script, 15*time.Second, er, nil, false)
}

func TestStream_InfluxQL_Float(t *testing.T) {

	type testCase struct {
		Method        string
		Args          string
		ER            kapacitor.Result
		UsePointTimes bool
	}

	var scriptTmpl = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(10s)
		.every(10s)
	|{{ .Method }}({{ .Args }})
		{{ if .UsePointTimes }}.usePointTimes(){{ end }}
	|httpOut('TestStream_InfluxQL_Float')
`
	endTime := time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC)
	testCases := []testCase{
		testCase{
			Method: "sum",
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
			Method: "count",
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
			Method: "distinct",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "distinct"},
						Values: [][]interface{}{
							{
								endTime,
								98.0,
							},
							{
								endTime,
								91.0,
							},
							{
								endTime,
								95.0,
							},
							{
								endTime,
								93.0,
							},
							{
								endTime,
								92.0,
							},
							{
								endTime,
								96.0,
							},
						},
					},
				},
			},
		},
		testCase{
			Method: "mean",
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
			Method: "median",
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
			Method:        "min",
			UsePointTimes: true,
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method: "min",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method:        "max",
			UsePointTimes: true,
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method: "max",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method: "spread",
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
			Method: "stddev",
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
			Method:        "first",
			UsePointTimes: true,
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method: "first",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method:        "last",
			UsePointTimes: true,
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method: "last",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method: "percentile",
			Args:   "'value', 50.0",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method:        "top",
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
			Method: "top",
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
			Method:        "bottom",
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
			Method: "bottom",
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
		testStreamerWithOutput(
			t,
			"TestStream_InfluxQL_Float",
			script.String(),
			13*time.Second,
			tc.ER,
			nil,
			false,
		)
	}
}

func TestStream_InfluxQL_Integer(t *testing.T) {
	type testCase struct {
		Method        string
		Args          string
		ER            kapacitor.Result
		UsePointTimes bool
	}

	var scriptTmpl = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(10s)
		.every(10s)
	|{{ .Method }}({{ .Args }})
		{{ if .UsePointTimes }}.usePointTimes(){{ end }}
	|httpOut('TestStream_InfluxQL_Integer')
`
	endTime := time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC)
	testCases := []testCase{
		testCase{
			Method: "sum",
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
			Method: "count",
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
			Method: "distinct",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "distinct"},
						Values: [][]interface{}{
							{
								endTime,
								98.0,
							},
							{
								endTime,
								91.0,
							},
							{
								endTime,
								95.0,
							},
							{
								endTime,
								93.0,
							},
							{
								endTime,
								92.0,
							},
							{
								endTime,
								96.0,
							},
						},
					},
				},
			},
		},
		testCase{
			Method: "mean",
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
			Method: "median",
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
			Method:        "min",
			UsePointTimes: true,
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method: "min",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method:        "max",
			UsePointTimes: true,
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method: "max",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method: "spread",
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
			Method: "stddev",
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
			Method:        "first",
			UsePointTimes: true,
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method: "first",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method:        "last",
			UsePointTimes: true,
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method: "last",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method: "percentile",
			Args:   "'value', 50.0",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
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
			Method:        "top",
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
			Method: "top",
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
			Method:        "bottom",
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
			Method: "bottom",
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
		testStreamerWithOutput(
			t,
			"TestStream_InfluxQL_Integer",
			script.String(),
			13*time.Second,
			tc.ER,
			nil,
			false,
		)
	}
}
func TestStream_InfluxQL_String(t *testing.T) {
	type testCase struct {
		Method        string
		Args          string
		ER            kapacitor.Result
		UsePointTimes bool
	}

	var scriptTmpl = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(10s)
		.every(10s)
	|{{ .Method }}({{ .Args }})
		{{ if .UsePointTimes }}.usePointTimes(){{ end }}
	|httpOut('TestStream_InfluxQL_String')
`
	endTime := time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC)
	testCases := []testCase{
		testCase{
			Method: "count",
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
			Method: "distinct",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "distinct"},
						Values: [][]interface{}{
							{
								endTime,
								"98",
							},
							{
								endTime,
								"91",
							},
							{
								endTime,
								"95",
							},
							{
								endTime,
								"93",
							},
							{
								endTime,
								"92",
							},
							{
								endTime,
								"96",
							},
						},
					},
				},
			},
		},
		testCase{
			Method:        "first",
			UsePointTimes: true,
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "first"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
							"98",
						}},
					},
				},
			},
		},
		testCase{
			Method: "first",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "first"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							"98",
						}},
					},
				},
			},
		},
		testCase{
			Method:        "last",
			UsePointTimes: true,
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "last"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
							"95",
						}},
					},
				},
			},
		},
		testCase{
			Method: "last",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "last"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							"95",
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
		if tc.Args == "" {
			tc.Args = "'value'"
		}
		tmpl.Execute(&script, tc)
		testStreamerWithOutput(
			t,
			"TestStream_InfluxQL_String",
			script.String(),
			13*time.Second,
			tc.ER,
			nil,
			false,
		)
	}
}

func TestStream_InfluxQL_Boolean(t *testing.T) {
	type testCase struct {
		Method        string
		Args          string
		ER            kapacitor.Result
		UsePointTimes bool
	}

	var scriptTmpl = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(10s)
		.every(10s)
	|{{ .Method }}({{ .Args }})
		{{ if .UsePointTimes }}.usePointTimes(){{ end }}
	|httpOut('TestStream_InfluxQL_Boolean')
`
	endTime := time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC)
	testCases := []testCase{
		testCase{
			Method: "count",
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
			Method: "distinct",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    nil,
						Columns: []string{"time", "distinct"},
						Values: [][]interface{}{
							{
								endTime,
								false,
							},
							{
								endTime,
								true,
							},
						},
					},
				},
			},
		},
		testCase{
			Method:        "first",
			UsePointTimes: true,
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "first"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
							false,
						}},
					},
				},
			},
		},
		testCase{
			Method: "first",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "first"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							false,
						}},
					},
				},
			},
		},
		testCase{
			Method:        "last",
			UsePointTimes: true,
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "last"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
							true,
						}},
					},
				},
			},
		},
		testCase{
			Method: "last",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "last"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							true,
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
		if tc.Args == "" {
			tc.Args = "'value'"
		}
		tmpl.Execute(&script, tc)
		testStreamerWithOutput(
			t,
			"TestStream_InfluxQL_Boolean",
			script.String(),
			13*time.Second,
			tc.ER,
			nil,
			false,
		)
	}
}

func TestStream_CustomFunctions(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	@customFunc()
		.opt1('count')
		.opt2(FALSE, 1, 1.0, '1.0', 1s)
	|httpOut('TestStream_CustomFunctions')
`

	udfService := UDFService{}
	udfService.ListFunc = func() []string {
		return []string{"customFunc"}
	}
	udfService.InfoFunc = func(name string) (info udf.Info, ok bool) {
		if name != "customFunc" {
			return
		}
		info.Wants = udf.EdgeType_STREAM
		info.Provides = udf.EdgeType_STREAM
		info.Options = map[string]*udf.OptionInfo{
			"opt1": {
				ValueTypes: []udf.ValueType{udf.ValueType_STRING},
			},
			"opt2": {
				ValueTypes: []udf.ValueType{
					udf.ValueType_BOOL,
					udf.ValueType_INT,
					udf.ValueType_DOUBLE,
					udf.ValueType_STRING,
					udf.ValueType_DURATION,
				},
			},
		}
		return
	}
	uio := udf_test.NewIO()
	udfService.CreateFunc = func(name string, l *log.Logger, abortCallback func()) (udf.Interface, error) {
		if name != "customFunc" {
			return nil, fmt.Errorf("unknown function %s", name)
		}
		return udf_test.New(uio, l), nil
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		req := <-uio.Requests
		i, ok := req.Message.(*udf.Request_Init)
		if !ok {
			t.Error("expected init message")
		}
		init := i.Init

		if got, exp := len(init.Options), 2; got != exp {
			t.Fatalf("unexpected number of options in init request, got %d exp %d", got, exp)
		}
		for i, opt := range init.Options {
			exp := &udf.Option{}
			switch i {
			case 0:
				exp.Name = "opt1"
				exp.Values = []*udf.OptionValue{
					{
						Type:  udf.ValueType_STRING,
						Value: &udf.OptionValue_StringValue{"count"},
					},
				}
			case 1:
				exp.Name = "opt2"
				exp.Values = []*udf.OptionValue{
					{
						Type:  udf.ValueType_BOOL,
						Value: &udf.OptionValue_BoolValue{false},
					},
					{
						Type:  udf.ValueType_INT,
						Value: &udf.OptionValue_IntValue{1},
					},
					{
						Type:  udf.ValueType_DOUBLE,
						Value: &udf.OptionValue_DoubleValue{1.0},
					},
					{
						Type:  udf.ValueType_STRING,
						Value: &udf.OptionValue_StringValue{"1.0"},
					},
					{
						Type:  udf.ValueType_DURATION,
						Value: &udf.OptionValue_DurationValue{int64(time.Second)},
					},
				}
			}
			if !reflect.DeepEqual(exp, opt) {
				t.Errorf("unexpected init option %d\ngot %v\nexp %v", i, opt, exp)
			}
		}

		resp := &udf.Response{
			Message: &udf.Response_Init{
				Init: &udf.InitResponse{
					Success: true,
				},
			},
		}
		uio.Responses <- resp

		// read all requests and wait till the chan is closed
		for req := range uio.Requests {
			p, ok := req.Message.(*udf.Request_Point)
			if ok {
				pt := p.Point
				resp := &udf.Response{
					Message: &udf.Response_Point{
						Point: &udf.Point{
							Name:         pt.Name,
							Time:         pt.Time,
							Group:        pt.Group,
							Tags:         pt.Tags,
							FieldsDouble: map[string]float64{"customField": 42.0},
						},
					},
				}
				uio.Responses <- resp
			}
		}

		close(uio.Responses)

		if err := <-uio.ErrC; err != nil {
			t.Error(err)
		}
	}()

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "customField"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					42.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_CustomFunctions", script, 15*time.Second, er, udfService, false)
	<-done
}

func TestStream_Alert(t *testing.T) {

	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := kapacitor.AlertData{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&ad)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)
		expAd := kapacitor.AlertData{
			ID:      "kapacitor/cpu/serverA",
			Message: "kapacitor/cpu/serverA is CRITICAL",
			Details: "details",
			Time:    time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
			Level:   kapacitor.CritAlert,
			Data: influxql.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA"},
						Columns: []string{"time", "count"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
							10.0,
						}},
					},
				},
			},
		}
		if eq, msg := compareAlertData(expAd, ad); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()

	var script = `
var infoThreshold = 6.0
var warnThreshold = 7.0
var critThreshold = 8.0

stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.details('details')
		.idField('id')
		.idTag('id')
		.levelField('level')
		.levelTag('level')
		.info(lambda: "count" > infoThreshold)
		.warn(lambda: "count" > warnThreshold)
		.crit(lambda: "count" > critThreshold)
		.post('` + ts.URL + `')
	|log()
	|httpOut('TestStream_Alert')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA", "level": "CRITICAL", "id": "kapacitor/cpu/serverA"},
				Columns: []string{"time", "count", "id", "level"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
					"kapacitor/cpu/serverA",
					"CRITICAL",
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Alert", script, 13*time.Second, er, nil, false)

	if rc := atomic.LoadInt32(&requestCount); rc != 1 {
		t.Errorf("got %v exp %v", rc, 1)
	}
}

func TestStream_AlertDuration(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := kapacitor.AlertData{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&ad)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		var expAd kapacitor.AlertData
		rc := atomic.LoadInt32(&requestCount)
		switch rc {
		case 1:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is CRITICAL",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
				Duration: 0,
				Level:    kapacitor.CritAlert,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "idle"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
								9.0,
							}},
						},
					},
				},
			}
		case 2:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is WARNING",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
				Duration: 2 * time.Second,
				Level:    kapacitor.WarnAlert,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "idle"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
								8.0,
							}},
						},
					},
				},
			}
		case 3:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is OK",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
				Duration: 4 * time.Second,
				Level:    kapacitor.OKAlert,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "idle"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
								6.0,
							}},
						},
					},
				},
			}
		case 4:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is WARNING",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
				Duration: 0,
				Level:    kapacitor.WarnAlert,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "idle"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
								8.0,
							}},
						},
					},
				},
			}
		case 5:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is OK",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
				Duration: 3 * time.Second,
				Level:    kapacitor.OKAlert,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "idle"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
								3.0,
							}},
						},
					},
				},
			}
		}
		if eq, msg := compareAlertData(expAd, ad); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()

	var script = `
var warnThreshold = 7.0
var critThreshold = 8.0

stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.details('details')
		.durationField('duration')
		.warn(lambda: "value" > warnThreshold)
		.crit(lambda: "value" > critThreshold)
		.stateChangesOnly()
		.post('` + ts.URL + `')
	|httpOut('TestStream_AlertDuration')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA", "type": "idle"},
				Columns: []string{"time", "duration", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
					float64(3 * time.Second),
					3.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_AlertDuration", script, 13*time.Second, er, nil, false)

	if exp, rc := 5, int(atomic.LoadInt32(&requestCount)); rc != exp {
		t.Errorf("got %v exp %v", rc, exp)
	}
}

func TestStream_AlertSensu(t *testing.T) {
	requestCount := int32(0)
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	listen, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer listen.Close()
	go func() {
		for {
			conn, err := listen.Accept()
			if err != nil {
				return
			}
			func() {
				defer conn.Close()

				atomic.AddInt32(&requestCount, 1)
				type postData struct {
					Name   string `json:"name"`
					Source string `json:"source"`
					Output string `json:"output"`
					Status int    `json:"status"`
				}
				pd := postData{}
				dec := json.NewDecoder(conn)
				dec.Decode(&pd)

				if exp := "Kapacitor"; pd.Source != exp {
					t.Errorf("unexpected source got %s exp %s", pd.Source, exp)
				}

				if exp := "kapacitor.cpu.serverA is CRITICAL"; pd.Output != exp {
					t.Errorf("unexpected text got %s exp %s", pd.Output, exp)
				}

				if exp := "kapacitor.cpu.serverA"; pd.Name != exp {
					t.Errorf("unexpected text got %s exp %s", pd.Name, exp)
				}

				if exp := 2; pd.Status != exp {
					t.Errorf("unexpected status got %v exp %v", pd.Status, exp)
				}
			}()
		}
	}()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor.{{ .Name }}.{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.sensu()
`

	clock, et, replayErr, tm := testStreamer(t, "TestStream_Alert", script, nil)
	defer tm.Close()

	c := sensu.NewConfig()
	c.Addr = listen.Addr().String()
	c.Source = "Kapacitor"
	sl := sensu.NewService(c, logService.NewLogger("[test_sensu] ", log.LstdFlags))
	tm.SensuService = sl

	err = fastForwardTask(clock, et, replayErr, tm, 13*time.Second)
	if err != nil {
		t.Error(err)
	}

	if rc := atomic.LoadInt32(&requestCount); rc != 1 {
		t.Errorf("unexpected requestCount got %d exp 1", rc)
	}
}

func TestStream_AlertSlack(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		type postData struct {
			Channel     string `json:"channel"`
			Username    string `json:"username"`
			Text        string `json:"text"`
			Attachments []struct {
				Fallback string `json:"fallback"`
				Color    string `json:"color"`
				Text     string `json:"text"`
			} `json:"attachments"`
		}
		pd := postData{}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&pd)
		if exp := "/test/slack/url"; r.URL.String() != exp {
			t.Errorf("unexpected url got %s exp %s", r.URL.String(), exp)
		}
		if rc := atomic.LoadInt32(&requestCount); rc == 1 {
			if exp := "#alerts"; pd.Channel != exp {
				t.Errorf("unexpected channel got %s exp %s", pd.Channel, exp)
			}
		} else if rc := atomic.LoadInt32(&requestCount); rc == 2 {
			if exp := "@jim"; pd.Channel != exp {
				t.Errorf("unexpected channel got %s exp %s", pd.Channel, exp)
			}
		}
		if exp := "kapacitor"; pd.Username != exp {
			t.Errorf("unexpected username got %s exp %s", pd.Username, exp)
		}
		if exp := ""; pd.Text != exp {
			t.Errorf("unexpected text got %s exp %s", pd.Text, exp)
		}
		if len(pd.Attachments) != 1 {
			t.Errorf("unexpected attachments got %v", pd.Attachments)
		} else {
			exp := "kapacitor/cpu/serverA is CRITICAL"
			if pd.Attachments[0].Fallback != exp {
				t.Errorf("unexpected fallback got %s exp %s", pd.Attachments[0].Fallback, exp)
			}
			if pd.Attachments[0].Text != exp {
				t.Errorf("unexpected text got %s exp %s", pd.Attachments[0].Text, exp)
			}
			if exp := "danger"; pd.Attachments[0].Color != exp {
				t.Errorf("unexpected color got %s exp %s", pd.Attachments[0].Color, exp)
			}
		}
	}))
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.slack()
		.channel('#alerts')
		.slack()
		.channel('@jim')
`

	clock, et, replayErr, tm := testStreamer(t, "TestStream_Alert", script, nil)
	defer tm.Close()

	c := slack.NewConfig()
	c.URL = ts.URL + "/test/slack/url"
	c.Channel = "#channel"
	sl := slack.NewService(c, logService.NewLogger("[test_slack] ", log.LstdFlags))
	tm.SlackService = sl

	err := fastForwardTask(clock, et, replayErr, tm, 13*time.Second)
	if err != nil {
		t.Error(err)
	}

	if rc := atomic.LoadInt32(&requestCount); rc != 2 {
		t.Errorf("unexpected requestCount got %d exp 2", rc)
	}
}

func TestStream_AlertHipChat(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		type postData struct {
			From    string `json:"from"`
			Message string `json:"message"`
			Color   string `json:"color"`
			Notify  bool   `json:"notify"`
		}
		pd := postData{}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&pd)

		if rc := atomic.LoadInt32(&requestCount); rc == 1 {
			if exp := "/1234567/notification?auth_token=testtoken1234567"; r.URL.String() != exp {
				t.Errorf("unexpected url got %s exp %s", r.URL.String(), exp)
			}
		} else if rc := atomic.LoadInt32(&requestCount); rc == 2 {
			if exp := "/Test%20Room/notification?auth_token=testtokenTestRoom"; r.URL.String() != exp {
				t.Errorf("unexpected url got %s exp %s", r.URL.String(), exp)
			}
		}
		if exp := "kapacitor"; pd.From != exp {
			t.Errorf("unexpected username got %s exp %s", pd.From, exp)
		}
		if exp := "kapacitor/cpu/serverA is CRITICAL"; pd.Message != exp {
			t.Errorf("unexpected text got %s exp %s", pd.Message, exp)
		}
		if exp := "red"; pd.Color != exp {
			t.Errorf("unexpected color got %s exp %s", pd.Color, exp)
		}
		if exp := true; pd.Notify != exp {
			t.Errorf("unexpected notify got %t exp %t", pd.Notify, exp)
		}
	}))
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.hipChat()
			.room('1234567')
			.token('testtoken1234567')
		.hipChat()
			.room('Test Room')
			.token('testtokenTestRoom')
`

	clock, et, replayErr, tm := testStreamer(t, "TestStream_Alert", script, nil)
	defer tm.Close()

	c := hipchat.NewConfig()
	c.URL = ts.URL
	c.Room = "1231234"
	c.Token = "testtoken1231234"
	sl := hipchat.NewService(c, logService.NewLogger("[test_hipchat] ", log.LstdFlags))
	tm.HipChatService = sl

	err := fastForwardTask(clock, et, replayErr, tm, 13*time.Second)
	if err != nil {
		t.Error(err)
	}

	if rc := atomic.LoadInt32(&requestCount); rc != 2 {
		t.Errorf("unexpected requestCount got %d exp 2", rc)
	}
}

func TestStream_AlertAlerta(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		type postData struct {
			Resource    string   `json:"resource"`
			Event       string   `json:"event"`
			Group       string   `json:"group"`
			Environment string   `json:"environment"`
			Text        string   `json:"text"`
			Origin      string   `json:"origin"`
			Service     []string `json:"service"`
			Value       string   `json:"value"`
		}
		pd := postData{}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&pd)

		if rc := atomic.LoadInt32(&requestCount); rc == 1 {
			if exp := "/alert?api-key=testtoken1234567"; r.URL.String() != exp {
				t.Errorf("unexpected url got %s exp %s", r.URL.String(), exp)
			}
			if exp := "cpu"; pd.Resource != exp {
				t.Errorf("unexpected resource got %s exp %s", pd.Resource, exp)
			}
			if exp := "serverA"; pd.Event != exp {
				t.Errorf("unexpected event got %s exp %s", pd.Event, exp)
			}
			if exp := "production"; pd.Environment != exp {
				t.Errorf("unexpected environment got %s exp %s", pd.Environment, exp)
			}
			if exp := "host=serverA,"; pd.Group != exp {
				t.Errorf("unexpected group got %s exp %s", pd.Group, exp)
			}
			if exp := ""; pd.Value != exp {
				t.Errorf("unexpected value got %s exp %s", pd.Value, exp)
			}
			if exp := []string{"cpu"}; !reflect.DeepEqual(pd.Service, exp) {
				t.Errorf("unexpected service got %s exp %s", pd.Service, exp)
			}
			if exp := "Kapacitor"; pd.Origin != exp {
				t.Errorf("unexpected origin got %s exp %s", pd.Origin, exp)
			}
		} else {
			if exp := "/alert?api-key=anothertesttoken"; r.URL.String() != exp {
				t.Errorf("unexpected url got %s exp %s", r.URL.String(), exp)
			}
			if exp := "resource: serverA"; pd.Resource != exp {
				t.Errorf("unexpected resource got %s exp %s", pd.Resource, exp)
			}
			if exp := "event: TestStream_Alert"; pd.Event != exp {
				t.Errorf("unexpected event got %s exp %s", pd.Event, exp)
			}
			if exp := "serverA"; pd.Environment != exp {
				t.Errorf("unexpected environment got %s exp %s", pd.Environment, exp)
			}
			if exp := "serverA"; pd.Group != exp {
				t.Errorf("unexpected group got %s exp %s", pd.Group, exp)
			}
			if exp := "10"; pd.Value != exp {
				t.Errorf("unexpected value got %s exp %s", pd.Value, exp)
			}
			if exp := []string{"serviceA", "serviceB"}; !reflect.DeepEqual(pd.Service, exp) {
				t.Errorf("unexpected service got %s exp %s", pd.Service, exp)
			}
			if exp := "override"; pd.Origin != exp {
				t.Errorf("unexpected origin got %s exp %s", pd.Origin, exp)
			}
		}
		if exp := "kapacitor/cpu/serverA is CRITICAL @1971-01-01 00:00:10 +0000 UTC"; pd.Text != exp {
			t.Errorf("unexpected text got %s exp %s", pd.Text, exp)
		}
	}))
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('{{ index .Tags "host" }}')
		.message('kapacitor/{{ .Name }}/{{ index .Tags "host" }} is {{ .Level }} @{{.Time}}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.alerta()
			.token('testtoken1234567')
			.environment('production')
		.alerta()
			.token('anothertesttoken')
			.resource('resource: {{ index .Tags "host" }}')
			.event('event: {{ .TaskName }}')
			.environment('{{ index .Tags "host" }}')
			.origin('override')
			.group('{{ .ID }}')
			.value('{{ index .Fields "count" }}')
			.services('serviceA', 'serviceB')
`

	clock, et, replayErr, tm := testStreamer(t, "TestStream_Alert", script, nil)
	defer tm.Close()

	c := alerta.NewConfig()
	c.URL = ts.URL
	c.Origin = "Kapacitor"
	sl := alerta.NewService(c, logService.NewLogger("[test_alerta] ", log.LstdFlags))
	tm.AlertaService = sl

	err := fastForwardTask(clock, et, replayErr, tm, 13*time.Second)
	if err != nil {
		t.Error(err)
	}

	if rc := atomic.LoadInt32(&requestCount); rc != 2 {
		t.Errorf("unexpected requestCount got %d exp 2", rc)
	}
}

func TestStream_AlertOpsGenie(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)

		type postData struct {
			ApiKey      string                 `json:"apiKey"`
			Message     string                 `json:"message"`
			Entity      string                 `json:"entity"`
			Alias       string                 `json:"alias"`
			Note        int                    `json:"note"`
			Details     map[string]interface{} `json:"details"`
			Description interface{}            `json:"description"`
			Teams       []string               `json:"teams"`
			Recipients  []string               `json:"recipients"`
		}

		pd := postData{}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&pd)

		if exp := "CRITICAL"; pd.Details["Level"] != exp {
			t.Errorf("unexpected level got %s exp %s", pd.Details["level"], exp)
		}
		if exp := "kapacitor/cpu/serverA"; pd.Entity != exp {
			t.Errorf("unexpected entity got %s exp %s", pd.Entity, exp)
		}
		if exp := "kapacitor/cpu/serverA"; pd.Alias != exp {
			t.Errorf("unexpected alias got %s exp %s", pd.Alias, exp)
		}
		if exp := "kapacitor/cpu/serverA is CRITICAL"; pd.Message != exp {
			t.Errorf("unexpected entity id got %s exp %s", pd.Message, exp)
		}
		if exp := "Kapacitor"; pd.Details["Monitoring Tool"] != exp {
			t.Errorf("unexpected monitoring tool got %s exp %s", pd.Details["Monitoring Tool"], exp)
		}
		if pd.Description == nil {
			t.Error("unexpected description got nil")
		}
		if rc := atomic.LoadInt32(&requestCount); rc == 1 {
			if exp, l := 2, len(pd.Teams); l != exp {
				t.Errorf("unexpected teams count got %d exp %d", l, exp)
			}
			if exp := "test_team"; pd.Teams[0] != exp {
				t.Errorf("unexpected teams[0] got %s exp %s", pd.Teams[0], exp)
			}
			if exp := "another_team"; pd.Teams[1] != exp {
				t.Errorf("unexpected teams[1] got %s exp %s", pd.Teams[1], exp)
			}
			if exp, l := 2, len(pd.Recipients); l != exp {
				t.Errorf("unexpected recipients count got %d exp %d", l, exp)
			}
			if exp := "test_recipient"; pd.Recipients[0] != exp {
				t.Errorf("unexpected recipients[0] got %s exp %s", pd.Recipients[0], exp)
			}
			if exp := "another_recipient"; pd.Recipients[1] != exp {
				t.Errorf("unexpected recipients[1] got %s exp %s", pd.Recipients[1], exp)
			}
		} else if rc := atomic.LoadInt32(&requestCount); rc == 2 {
			if exp, l := 1, len(pd.Teams); l != exp {
				t.Errorf("unexpected teams count got %d exp %d", l, exp)
			}
			if exp := "test_team2"; pd.Teams[0] != exp {
				t.Errorf("unexpected teams[0] got %s exp %s", pd.Teams[0], exp)
			}
			if exp, l := 2, len(pd.Recipients); l != exp {
				t.Errorf("unexpected recipients count got %d exp %d", l, exp)
			}
			if exp := "test_recipient2"; pd.Recipients[0] != exp {
				t.Errorf("unexpected recipients[0] got %s exp %s", pd.Recipients[0], exp)
			}
			if exp := "another_recipient"; pd.Recipients[1] != exp {
				t.Errorf("unexpected recipients[1] got %s exp %s", pd.Recipients[1], exp)
			}
		}
	}))
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.opsGenie()
			.teams('test_team', 'another_team')
			.recipients('test_recipient', 'another_recipient')
		.opsGenie()
			.teams('test_team2' )
			.recipients('test_recipient2', 'another_recipient')
`

	clock, et, replayErr, tm := testStreamer(t, "TestStream_Alert", script, nil)
	defer tm.Close()
	c := opsgenie.NewConfig()
	c.URL = ts.URL
	c.APIKey = "api_key"
	og := opsgenie.NewService(c, logService.NewLogger("[test_og] ", log.LstdFlags))
	tm.OpsGenieService = og

	err := fastForwardTask(clock, et, replayErr, tm, 13*time.Second)
	if err != nil {
		t.Error(err)
	}

	if rc := atomic.LoadInt32(&requestCount); rc != 2 {
		t.Errorf("unexpected requestCount got %d exp 1", rc)
	}
}

func TestStream_AlertPagerDuty(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		type postData struct {
			ServiceKey  string      `json:"service_key"`
			EventType   string      `json:"event_type"`
			Description string      `json:"description"`
			Client      string      `json:"client"`
			ClientURL   string      `json:"client_url"`
			Details     interface{} `json:"details"`
		}
		pd := postData{}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&pd)
		if rc := atomic.LoadInt32(&requestCount); rc == 1 {
			if exp := "service_key"; pd.ServiceKey != exp {
				t.Errorf("unexpected service key got %s exp %s", pd.ServiceKey, exp)
			}
		} else if rc := atomic.LoadInt32(&requestCount); rc == 2 {
			if exp := "test_override_key"; pd.ServiceKey != exp {
				t.Errorf("unexpected service key got %s exp %s", pd.ServiceKey, exp)
			}
		}
		if exp := "trigger"; pd.EventType != exp {
			t.Errorf("unexpected event type got %s exp %s", pd.EventType, exp)
		}
		if exp := "CRITICAL alert for kapacitor/cpu/serverA"; pd.Description != exp {
			t.Errorf("unexpected description got %s exp %s", pd.Description, exp)
		}
		if exp := "kapacitor"; pd.Client != exp {
			t.Errorf("unexpected client got %s exp %s", pd.Client, exp)
		}
		if len(pd.ClientURL) == 0 {
			t.Errorf("unexpected client url got empty string")
		}
		if pd.Details == nil {
			t.Error("unexpected data got nil")
		}
	}))
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.message('{{ .Level }} alert for {{ .ID }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.pagerDuty()
		.pagerDuty()
			.serviceKey('test_override_key')
`

	clock, et, replayErr, tm := testStreamer(t, "TestStream_Alert", script, nil)
	defer tm.Close()
	c := pagerduty.NewConfig()
	c.URL = ts.URL
	c.ServiceKey = "service_key"
	pd := pagerduty.NewService(c, logService.NewLogger("[test_pd] ", log.LstdFlags))
	pd.HTTPDService = tm.HTTPDService
	tm.PagerDutyService = pd

	err := fastForwardTask(clock, et, replayErr, tm, 13*time.Second)
	if err != nil {
		t.Error(err)
	}

	if rc := atomic.LoadInt32(&requestCount); rc != 2 {
		t.Errorf("unexpected requestCount got %d exp 1", rc)
	}
}

func TestStream_AlertVictorOps(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		if rc := atomic.LoadInt32(&requestCount); rc == 1 {
			if exp, got := "/api_key/test_key", r.URL.String(); got != exp {
				t.Errorf("unexpected VO url got %s exp %s", got, exp)
			}
		} else if rc := atomic.LoadInt32(&requestCount); rc == 2 {
			if exp, got := "/api_key/test_key2", r.URL.String(); got != exp {
				t.Errorf("unexpected VO url got %s exp %s", got, exp)
			}
		}
		type postData struct {
			MessageType    string      `json:"message_type"`
			EntityID       string      `json:"entity_id"`
			StateMessage   string      `json:"state_message"`
			Timestamp      int         `json:"timestamp"`
			MonitoringTool string      `json:"monitoring_tool"`
			Data           interface{} `json:"data"`
		}
		pd := postData{}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&pd)
		if exp := "CRITICAL"; pd.MessageType != exp {
			t.Errorf("unexpected message type got %s exp %s", pd.MessageType, exp)
		}
		if exp := "kapacitor/cpu/serverA"; pd.EntityID != exp {
			t.Errorf("unexpected entity id got %s exp %s", pd.EntityID, exp)
		}
		if exp := "kapacitor/cpu/serverA is CRITICAL"; pd.StateMessage != exp {
			t.Errorf("unexpected state message got %s exp %s", pd.StateMessage, exp)
		}
		if exp := "kapacitor"; pd.MonitoringTool != exp {
			t.Errorf("unexpected monitoring tool got %s exp %s", pd.MonitoringTool, exp)
		}
		if exp := 31536010; pd.Timestamp != exp {
			t.Errorf("unexpected timestamp got %d exp %d", pd.Timestamp, exp)
		}
		if pd.Data == nil {
			t.Error("unexpected data got nil")
		}
	}))
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.victorOps()
			.routingKey('test_key')
		.victorOps()
			.routingKey('test_key2')
`

	clock, et, replayErr, tm := testStreamer(t, "TestStream_Alert", script, nil)
	defer tm.Close()
	c := victorops.NewConfig()
	c.URL = ts.URL
	c.APIKey = "api_key"
	c.RoutingKey = "routing_key"
	vo := victorops.NewService(c, logService.NewLogger("[test_vo] ", log.LstdFlags))
	tm.VictorOpsService = vo

	err := fastForwardTask(clock, et, replayErr, tm, 13*time.Second)
	if err != nil {
		t.Error(err)
	}

	if rc := atomic.LoadInt32(&requestCount); rc != 2 {
		t.Errorf("unexpected requestCount got %d exp 1", rc)
	}
}

func TestStream_AlertTalk(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		type postData struct {
			Title      string `json:"title"`
			Text       string `json:"text"`
			AuthorName string `json:"authorName"`
		}
		pd := postData{}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&pd)

		if exp := "Kapacitor"; pd.AuthorName != exp {
			t.Errorf("unexpected source got %s exp %s", pd.AuthorName, exp)
		}

		if exp := "kapacitor/cpu/serverA is CRITICAL"; pd.Text != exp {
			t.Errorf("unexpected text got %s exp %s", pd.Text, exp)
		}

		if exp := "kapacitor/cpu/serverA"; pd.Title != exp {
			t.Errorf("unexpected text got %s exp %s", pd.Title, exp)
		}

	}))
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.talk()
`

	clock, et, replayErr, tm := testStreamer(t, "TestStream_Alert", script, nil)
	defer tm.Close()

	c := talk.NewConfig()
	c.URL = ts.URL
	c.AuthorName = "Kapacitor"
	sl := talk.NewService(c, logService.NewLogger("[test_talk] ", log.LstdFlags))
	tm.TalkService = sl

	err := fastForwardTask(clock, et, replayErr, tm, 13*time.Second)
	if err != nil {
		t.Error(err)
	}

	if rc := atomic.LoadInt32(&requestCount); rc != 1 {
		t.Errorf("unexpected requestCount got %d exp 1", rc)
	}
}

func TestStream_AlertSigma(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := kapacitor.AlertData{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&ad)
		if err != nil {
			t.Fatal(err)
		}
		var expAd kapacitor.AlertData
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)
		if rc := atomic.LoadInt32(&requestCount); rc == 1 {
			expAd = kapacitor.AlertData{
				ID:      "cpu:nil",
				Message: "cpu:nil is INFO",
				Details: "cpu:nil is INFO",
				Time:    time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
				Level:   kapacitor.InfoAlert,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "idle"},
							Columns: []string{"time", "sigma", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
								2.469916402324427,
								16.0,
							}},
						},
					},
				},
			}
		} else {
			expAd = kapacitor.AlertData{
				ID:       "cpu:nil",
				Message:  "cpu:nil is OK",
				Details:  "cpu:nil is OK",
				Time:     time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
				Duration: time.Second,
				Level:    kapacitor.OKAlert,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "idle"},
							Columns: []string{"time", "sigma", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
								0.3053477916297622,
								93.4,
							}},
						},
					},
				},
			}
		}
		if eq, msg := compareAlertData(expAd, ad); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|eval(lambda: sigma("value"))
		.as('sigma')
		.keep()
	|alert()
		.details('{{ .Message }}')
		.info(lambda: "sigma" > 2.0)
		.warn(lambda: "sigma" > 3.0)
		.crit(lambda: "sigma" > 3.5)
		.post('` + ts.URL + `')
`

	testStreamerNoOutput(t, "TestStream_AlertSigma", script, 13*time.Second)

	if rc := atomic.LoadInt32(&requestCount); rc != 2 {
		t.Errorf("got %v exp %v", rc, 2)
	}
}

func TestStream_AlertComplexWhere(t *testing.T) {

	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := kapacitor.AlertData{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&ad)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		expAd := kapacitor.AlertData{
			ID:      "cpu:nil",
			Message: "cpu:nil is CRITICAL",
			Details: "",
			Time:    time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
			Level:   kapacitor.CritAlert,
			Data: influxql.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
							16.0,
						}},
					},
				},
			},
		}
		if eq, msg := compareAlertData(expAd, ad); !eq {
			t.Error(msg)
		}
	}))
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA' AND sigma("value") > 2)
	|alert()
		.details('')
		.crit(lambda: TRUE)
		.post('` + ts.URL + `')
`

	testStreamerNoOutput(t, "TestStream_AlertComplexWhere", script, 13*time.Second)

	if rc := atomic.LoadInt32(&requestCount); rc != 1 {
		t.Errorf("got %v exp %v", rc, 1)
	}
}

func TestStream_AlertStateChangesOnly(t *testing.T) {

	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
	}))
	defer ts.Close()
	var script = `
stream
	|from()
		.measurement('cpu')
	|alert()
		.crit(lambda: "value" < 93)
		.stateChangesOnly()
		.post('` + ts.URL + `')
`

	testStreamerNoOutput(t, "TestStream_AlertStateChangesOnly", script, 13*time.Second)

	// Only 4 points below 93 so 8 state changes.
	if rc := atomic.LoadInt32(&requestCount); rc != 8 {
		t.Errorf("got %v exp %v", rc, 5)
	}
}
func TestStream_AlertStateChangesOnlyExpired(t *testing.T) {

	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := kapacitor.AlertData{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&ad)
		if err != nil {
			t.Fatal(err)
		}
		//We don't care about the data for this test
		ad.Data = influxql.Result{}
		var expAd kapacitor.AlertData
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)
		if rc < 6 {
			expAd = kapacitor.AlertData{
				ID:       "cpu:nil",
				Message:  "cpu:nil is CRITICAL",
				Time:     time.Date(1971, 1, 1, 0, 0, int(rc)*2-1, 0, time.UTC),
				Duration: time.Duration(rc-1) * 2 * time.Second,
				Level:    kapacitor.CritAlert,
			}
		} else {
			expAd = kapacitor.AlertData{
				ID:       "cpu:nil",
				Message:  "cpu:nil is OK",
				Time:     time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
				Duration: 9 * time.Second,
				Level:    kapacitor.OKAlert,
			}
		}
		if eq, msg := compareAlertData(expAd, ad); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()
	var script = `
stream
	|from()
		.measurement('cpu')
	|alert()
		.crit(lambda: "value" < 97)
		.details('')
		.stateChangesOnly(2s)
		.post('` + ts.URL + `')
`

	testStreamerNoOutput(t, "TestStream_AlertStateChangesOnlyExpired", script, 13*time.Second)

	// 9 points below 97 once per second so 5 alerts + 1 recovery
	if exp, rc := 6, int(atomic.LoadInt32(&requestCount)); rc != exp {
		t.Errorf("got %v exp %v", rc, exp)
	}
}

func TestStream_AlertFlapping(t *testing.T) {

	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
	}))
	defer ts.Close()
	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|alert()
		.info(lambda: "value" < 95)
		.warn(lambda: "value" < 94)
		.crit(lambda: "value" < 93)
		.flapping(0.25, 0.50)
		.history(21)
		.post('` + ts.URL + `')
`

	testStreamerNoOutput(t, "TestStream_AlertFlapping", script, 13*time.Second)

	// Flapping detection should drop the last alerts.
	if rc := atomic.LoadInt32(&requestCount); rc != 9 {
		t.Errorf("got %v exp %v", rc, 9)
	}
}

func TestStream_InfluxDBOut(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|influxDBOut()
		.database('db')
		.retentionPolicy('rp')
		.measurement('m')
		.precision('s')
		.tag('key', 'value')
		.flushInterval(1ms)
`
	done := make(chan error, 1)
	var points []imodels.Point
	var database string
	var rp string
	var precision string

	influxdb := NewMockInfluxDBService(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ping" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
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

	clock, et, replayErr, tm := testStreamer(t, "TestStream_InfluxDBOut", script, nil)
	tm.InfluxDBService = influxdb
	defer tm.Close()

	err := fastForwardTask(clock, et, replayErr, tm, 15*time.Second)
	if err != nil {
		t.Error(err)
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
		if p.Fields()["count"] != int64(10) {
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

func TestStream_Selectors(t *testing.T) {

	var script = `
stream
	|from().measurement('cpu')
	.where(lambda: "host" == 'serverA')
	|window()
		.period(10s)
		.every(10s)
	|last('value')
	|httpOut('TestStream_Selectors')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA", "type": "idle"},
				Columns: []string{"time", "another", "last"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					5.0,
					95.3,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Selectors", script, 15*time.Second, er, nil, false)
}

func TestStream_TopSelector(t *testing.T) {

	var script = `
var topScores = stream
    |from()
		.measurement('scores')
		// Get the most recent score for each player
		.groupBy('game', 'player')
    |window()
        .period(2s)
        .every(2s)
        .align()
    |last('value')
    // Calculate the top 5 scores per game
    |groupBy('game')
    |top(5, 'last', 'player')

topScores
    |httpOut('top_scores')

topScores
    |sample(4s)
    |count('top')
    |httpOut('top_scores_sampled')
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

	clock, et, replayErr, tm := testStreamer(t, "TestStream_TopSelector", script, nil)
	defer tm.Close()

	err := fastForwardTask(clock, et, replayErr, tm, 10*time.Second)
	if err != nil {
		t.Error(err)
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
func testStreamer(
	t *testing.T,
	name,
	script string,
	udfService kapacitor.UDFService,
) (
	clock.Setter,
	*kapacitor.ExecutingTask,
	<-chan error,
	*kapacitor.TaskMaster,
) {
	if testing.Verbose() {
		wlog.SetLevel(wlog.DEBUG)
	} else {
		wlog.SetLevel(wlog.OFF)
	}

	// Create a new execution env
	tm := kapacitor.NewTaskMaster("testStreamer", logService)
	tm.HTTPDService = httpService
	tm.UDFService = udfService
	tm.TaskStore = taskStore{}
	tm.DeadmanService = deadman{}
	tm.Open()

	//Create the task
	task, err := tm.NewTask(name, script, kapacitor.StreamTask, dbrps, 0, nil)
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
	//Start the task
	et, err := tm.StartTask(task)
	if err != nil {
		t.Fatal(err)
	}

	// Replay test data to executor
	stream, err := tm.Stream(name)
	if err != nil {
		t.Fatal(err)
	}
	// Use 1971 so that we don't get true negatives on Epoch 0 collisions
	c := clock.New(time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC))

	replayErr := kapacitor.ReplayStreamFromIO(c, data, stream, false, "s")

	t.Log(string(et.Task.Dot()))
	return c, et, replayErr, tm
}

func fastForwardTask(
	clock clock.Setter,
	et *kapacitor.ExecutingTask,
	replayErr <-chan error,
	tm *kapacitor.TaskMaster,
	duration time.Duration,
) error {
	// Move time forward
	clock.Set(clock.Zero().Add(duration))
	// Wait till the replay has finished
	if err := <-replayErr; err != nil {
		return err
	}
	tm.Drain()
	et.StopStats()
	// Wait till the task is finished
	if err := et.Wait(); err != nil {
		return err
	}
	return nil
}

func testStreamerNoOutput(
	t *testing.T,
	name,
	script string,
	duration time.Duration,
) {
	clock, et, replayErr, tm := testStreamer(t, name, script, nil)
	err := fastForwardTask(clock, et, replayErr, tm, duration)
	if err != nil {
		t.Error(err)
	}
	defer tm.Close()
}

func testStreamerWithOutput(
	t *testing.T,
	name,
	script string,
	duration time.Duration,
	er kapacitor.Result,
	udfService kapacitor.UDFService,
	ignoreOrder bool,
) {
	clock, et, replayErr, tm := testStreamer(t, name, script, udfService)
	err := fastForwardTask(clock, et, replayErr, tm, duration)
	if err != nil {
		t.Error(err)
	}
	defer tm.Close()

	// Get the result
	output, err := et.GetOutput(name)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(output.Endpoint())
	if err != nil {
		t.Fatal(err)
	}

	// Assert we got the expected result
	result := kapacitor.ResultFromJSON(resp.Body)
	if ignoreOrder {
		if eq, msg := compareResultsIgnoreSeriesOrder(er, result); !eq {
			t.Error(msg)
		}
	} else {
		if eq, msg := compareResults(er, result); !eq {
			t.Error(msg)
		}
	}
}
