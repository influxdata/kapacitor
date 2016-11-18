package integrations

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/mail"
	"os"
	"path"
	"path/filepath"
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
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/command/commandtest"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/kapacitor/services/alerta"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/httpd"
	k8s "github.com/influxdata/kapacitor/services/k8s/client"
	"github.com/influxdata/kapacitor/services/logging/loggingtest"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/sensu"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/smtp/smtptest"
	"github.com/influxdata/kapacitor/services/talk"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/influxdata/kapacitor/udf"
	"github.com/influxdata/kapacitor/udf/test"
	"github.com/influxdata/wlog"
)

var httpService *httpd.Service
var logService = loggingtest.New()

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
	httpService = httpd.NewService(config, "localhost", logService.NewLogger("[http] ", log.LstdFlags), logService)
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

	testStreamerWithOutput(t, "TestStream_Derivative", script, 15*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_DerivativeZeroElapsed", script, 15*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_Derivative", script, 15*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_DerivativeNN", script, 15*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_DerivativeNN", script, 15*time.Second, er, false, nil)
}

func TestStream_HoltWinters(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('packets')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|holtWinters('value', 3, 0, 1s)
	|where(lambda: "host" == 'serverA')
	|httpOut('TestStream_HoltWinters')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    models.Tags{"host": "serverA"},
				Columns: []string{"time", "holtWinters"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
						1009.324690106368,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
						1009.7524349889708,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 12, 0, time.UTC),
						1010.105056042826,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_HoltWinters", script, 15*time.Second, er, false, nil)
}

func TestStream_HoltWintersWithFit(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('packets')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|holtWintersWithFit('value', 3, 0, 1s)
	|where(lambda: "host" == 'serverA')
	|httpOut('TestStream_HoltWinters')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    models.Tags{"host": "serverA"},
				Columns: []string{"time", "holtWinters"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						1000.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						1000.7349380776699,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						1001.8935462884633,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						1003.1750039651934,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						1004.4245269000132,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						1005.5685498251902,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						1006.5782508658309,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
						1007.4488388165385,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						1008.1877681696025,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
						1008.8080773333872,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
						1009.324690106368,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
						1009.7524349889708,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 12, 0, time.UTC),
						1010.105056042826,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_HoltWinters", script, 15*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_Elapsed", script, 15*time.Second, er, false, nil)
}

func TestStream_Difference(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('packets')
	|difference('value')
	|window()
		.period(10s)
		.every(10s)
	|max('difference')
	|httpOut('TestStream_Difference')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
					5.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Difference", script, 15*time.Second, er, false, nil)
}

func TestStream_MovingAverage(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('packets')
	|movingAverage('value', 4)
	|window()
		.period(10s)
		.every(10s)
	|httpOut('TestStream_MovingAverage')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "movingAverage"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						1001.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						1005.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						1008.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						1012.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
						1015.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						1016.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
						1017.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
						1018.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
						1019.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 12, 0, time.UTC),
						1020.5,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_MovingAverage", script, 16*time.Second, er, false, nil)
}

func TestStream_CumulativeSum(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('packets')
	|cumulativeSum('value')
	|window()
		.period(10s)
		.every(10s)
	|httpOut('TestStream_CumulativeSum')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "cumulativeSum"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						6.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						10.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						15.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
						21.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						28.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
						36.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_CumulativeSum", script, 13*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_WindowMissing", script, 13*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_WindowMissing", script, 13*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_Window", script, 13*time.Second, er, false, nil)
}

func TestStream_Window_Count(t *testing.T) {

	var script = `
var period = 10
var every = 10
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.periodCount(period)
		.everyCount(every)
	|httpOut('TestStream_Window_Count')
`

	count := 10
	values := make([][]interface{}, count)
	for i := 0; i < count; i++ {
		values[i] = []interface{}{
			time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
			"serverA",
			"idle",
			float64(i + 1),
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

	testStreamerWithOutput(t, "TestStream_Window_Count", script, 2*time.Second, er, false, nil)
}
func TestStream_Window_Count_Overlapping(t *testing.T) {

	var script = `
var period = 3
var every = 1
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.periodCount(period)
		.everyCount(every)
	|httpOut('TestStream_Window_Count')
`

	count := 3
	values := make([][]interface{}, count)
	for i := 0; i < count; i++ {
		values[i] = []interface{}{
			time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
			"serverA",
			"idle",
			float64(i + 10),
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

	testStreamerWithOutput(t, "TestStream_Window_Count", script, 2*time.Second, er, false, nil)
}

func TestStream_Window_Count_Every_1(t *testing.T) {

	var script = `
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.groupBy('host')
	|window()
		.periodCount(3)
		.everyCount(1)
	|count('value')
	|window()
		.periodCount(20)
		.everyCount(1)
	|httpOut('TestStream_Window_Count')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						2.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Window_Count", script, 2*time.Second, er, false, nil)
}

func TestStream_Window_Count_FillPeriod(t *testing.T) {

	var script = `
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.groupBy('host')
	|window()
		.periodCount(4)
		.everyCount(1)
		.fillPeriod()
	|count('value')
	|window()
		.periodCount(20)
		.everyCount(1)
	|httpOut('TestStream_Window_Count')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Window_Count", script, 2*time.Second, er, false, nil)
}

func TestStream_Window_Every_0(t *testing.T) {

	var script = `
var period = 10s
// Emit the window on every point
var every = 0s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.groupBy('host')
	|window()
		.period(period)
		.every(every)
	|count('value')
	|window()
		.period(10s)
		.every(10s)
	|httpOut('TestStream_Window')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						2.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						6.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						7.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
						8.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						9.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
						10.0,
					},
				},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverB"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						2.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						6.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						7.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
						8.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						9.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
						10.0,
					},
				},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverC"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						2.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Window", script, 13*time.Second, er, false, nil)
}

func TestStream_Window_Every_0_FillPeriod(t *testing.T) {

	var script = `
var period = 5s
// Emit the window on every point
var every = 0s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.groupBy('host')
	|window()
		.period(period)
		.every(every)
		.fillPeriod()
	|count('value')
	|window()
		.period(10s)
		.every(0s)
	|httpOut('TestStream_Window')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
						5.0,
					},
				},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverB"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
						5.0,
					},
				},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverC"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
						1.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Window", script, 13*time.Second, er, false, nil)
}

func TestStream_Window_Overlapping(t *testing.T) {

	var script = `
var period = 14s
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
	|httpOut('TestStream_Window_FillPeriod')
`

	nums := []float64{
		93.1,
		97.1,
		92.6,
		95.6,
		93.1,
		92.6,
		95.8,
		92.7,
		96.0,
		93.4,
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

	testStreamerWithOutput(t, "TestStream_Window_FillPeriod", script, 16*time.Second, er, false, nil)
}

func TestStream_Window_FillPeriod(t *testing.T) {

	var script = `
var period = 14s
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
		.fillPeriod()
	|httpOut('TestStream_Window_FillPeriod')
`

	nums := []float64{
		93.1,
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
		96.4,
		95.1,
		91.1,
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

	testStreamerWithOutput(t, "TestStream_Window_FillPeriod", script, 16*time.Second, er, false, nil)
}
func TestStream_Window_FillPeriod_Aligned(t *testing.T) {

	var script = `
var period = 14s
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
		.fillPeriod()
		.align()
	|httpOut('TestStream_Window_FillPeriod_Aligned')
`

	nums := []float64{
		95.8,
		92.7,
		96.0,
		93.4,
		95.3,
		96.4,
		95.1,
		91.1,
		95.7,
		96.2,
		96.6,
		91.2,
		98.2,
		96.1,
	}

	values := make([][]interface{}, len(nums))
	for i, num := range nums {
		values[i] = []interface{}{
			time.Date(1971, 1, 1, 0, 0, i+6, 0, time.UTC),
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

	testStreamerWithOutput(t, "TestStream_Window_FillPeriod_Aligned", script, 21*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_Shift", script, 15*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_Shift", script, 15*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_Shift", script, 15*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_Shift", script, 15*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_SimpleMR", script, 15*time.Second, er, false, nil)
}

func TestStream_Eval_AllTypes(t *testing.T) {
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

	testStreamerWithOutput(t, "TestStream_EvalAllTypes", script, 2*time.Second, er, false, nil)
}

func TestStream_Eval_KeepAll(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
	|eval(lambda: "value0" + "value1", lambda: "value0" - "value1")
		.as( 'pos', 'neg')
		.keep()
	|httpOut('TestStream_Eval_Keep')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "types",
				Tags:    nil,
				Columns: []string{"time", "neg", "pos", "value0", "value1"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					-1.0,
					1.0,
					0.0,
					1.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Eval_Keep", script, 2*time.Second, er, false, nil)
}

func TestStream_Eval_KeepSome(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
	|eval(lambda: "value0" + "value1", lambda: "value0" - "value1")
		.as( 'pos', 'neg')
		.keep('value0', 'pos', 'neg', 'other')
	|httpOut('TestStream_Eval_KeepSome')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "types",
				Tags:    nil,
				Columns: []string{"time", "neg", "other", "pos", "value0"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					-1.0,
					5.0,
					1.0,
					0.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Eval_KeepSome", script, 2*time.Second, er, false, nil)
}

func TestStream_Eval_KeepSomeWithHidden(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
	|eval(lambda: "value0" + "value1", lambda: "pos" - "value1")
		.as( 'pos', 'zero')
		.keep('value0', 'zero')
	|httpOut('TestStream_Eval_Keep')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "types",
				Tags:    nil,
				Columns: []string{"time", "value0", "zero"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					0.0,
					0.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Eval_Keep", script, 2*time.Second, er, false, nil)
}

func TestStream_Eval_Tags(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
	|eval(lambda: string("value"))
		.as('value')
		.tags('value')
	|groupBy('value')
	|httpOut('TestStream_Eval_Tags')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "types",
				Tags:    map[string]string{"value": "0"},
				Columns: []string{"time"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
				}},
			},
			{
				Name:    "types",
				Tags:    map[string]string{"value": "1"},
				Columns: []string{"time"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Eval_Tags", script, 2*time.Second, er, true, nil)
}

func TestStream_Eval_Tags_Keep(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
	|eval(lambda: string("value"))
		.as('value')
		.tags('value')
		.keep()
	|groupBy('value')
	|httpOut('TestStream_Eval_Tags')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "types",
				Tags:    map[string]string{"value": "0"},
				Columns: []string{"time", "another", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					2.0,
					"0",
				}},
			},
			{
				Name:    "types",
				Tags:    map[string]string{"value": "1"},
				Columns: []string{"time", "another", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					2.0,
					"1",
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Eval_Tags", script, 2*time.Second, er, true, nil)
}

func TestStream_Eval_Tags_KeepSome(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
	|eval(lambda: string("value"))
		.as('value_tag')
		.tags('value_tag')
		.keep('value', 'another')
	|groupBy('value_tag')
	|httpOut('TestStream_Eval_Tags')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "types",
				Tags:    map[string]string{"value_tag": "0"},
				Columns: []string{"time", "another", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					2.0,
					0.0,
				}},
			},
			{
				Name:    "types",
				Tags:    map[string]string{"value_tag": "1"},
				Columns: []string{"time", "another", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					2.0,
					1.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Eval_Tags", script, 2*time.Second, er, true, nil)
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

	testStreamerWithOutput(t, "TestStream_EvalGroups", script, 3*time.Second, er, false, nil)
}

func TestStream_Eval_Time(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
		.groupBy('group')
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

	testStreamerWithOutput(t, "TestStream_Eval_Time", script, 2*time.Hour, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_Default", script, 15*time.Second, er, false, nil)
}

func TestStream_Delete(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('cpu')
	|delete()
		.field('anothervalue')
		.tag('type')
	|groupBy(*)
	|httpOut('TestStream_Delete')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					9.0,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverB"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					6.0,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverC"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					3.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Delete", script, 15*time.Second, er, true, nil)
}

func TestStream_Delete_GroupBy(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('cpu')
		.groupBy('host', 'type')
	|delete()
		.field('anothervalue')
		.tag('type')
	|window()
		.period(2s)
		.every(2s)
	|sum('value')
		.as('value')
	|httpOut('TestStream_Delete_GroupBy')
`
	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
					18.0,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverB"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
					12.0,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverC"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
					6.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Delete_GroupBy", script, 15*time.Second, er, true, nil)
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

	testStreamerWithOutput(t, "TestStream_AllMeasurements", script, 15*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_SimpleMR", script, 15*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_BatchGroupBy", script, 15*time.Second, er, true, nil)
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

	testStreamerWithOutput(t, "TestStream_BatchGroupBy", script, 15*time.Second, er, true, nil)
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

	testStreamerWithOutput(t, "TestStream_SimpleMR", script, 15*time.Second, er, false, nil)
}

func TestStream_Where_NoSideEffect(t *testing.T) {

	var script = `
var data = stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.where(lambda: "host" != 'serverB')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|where(lambda: "count" > 0)

// Unused where clause should not side-effect
data
	|where(lambda: FALSE)

data
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

	testStreamerWithOutput(t, "TestStream_SimpleMR", script, 15*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_SimpleMR", script, 15*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_SimpleMR", script, 15*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_GroupBy", script, 13*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_GroupByWhere", script, 13*time.Second, er, true, nil)
}

func TestStream_GroupByMeasurement(t *testing.T) {

	var script = `
stream
	|from()
		.groupBy('service')
		.groupByMeasurement()
	|window()
		.period(10s)
		.every(10s)
	|sum('value')
	|httpOut('TestStream_GroupByMeasurement')
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
			{
				Name:    "disk",
				Tags:    map[string]string{"service": "sda"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					810.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_GroupByMeasurement", script, 13*time.Second, er, true, nil)
}

func TestStream_Flatten(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('request_latency')
		.groupBy('dc')
	|flatten()
		.on('service', 'host')
		.tolerance(1s)
    |httpOut('TestStream_Flatten')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A"},
				Columns: []string{"time", "auth.server01.value", "auth.server02.value", "cart.server01.value", "cart.server02.value", "log.server01.value", "log.server02.value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					700.0,
					702.0,
					800.0,
					802.0,
					600.0,
					602.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B"},
				Columns: []string{"time", "auth.server01.value", "auth.server02.value", "cart.server01.value", "cart.server02.value", "log.server01.value", "log.server02.value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					750.0,
					752.0,
					850.0,
					852.0,
					650.0,
					652.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Flatten", script, 13*time.Second, er, true, nil)
}

func TestStream_Combine_All(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('request_latency')
		.groupBy('dc')
	|combine(lambda: TRUE, lambda: TRUE)
		.as('first', 'second')
		.tolerance(1s)
		.delimiter('.')
	|groupBy('first.service', 'second.service', 'dc')
	|eval(lambda: "first.value" / "second.value")
		.as('ratio')
    |httpOut('TestStream_Combine')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "second.service": "log", "first.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.0 / 6.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "second.service": "cart", "first.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.0 / 8.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "second.service": "cart", "first.service": "log"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					6.0 / 8.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "second.service": "log", "first.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.5 / 6.5,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "second.service": "cart", "first.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.5 / 8.5,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "second.service": "cart", "first.service": "log"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					6.5 / 8.5,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Combine", script, 13*time.Second, er, true, nil)
}

func TestStream_Combine_Filtered(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('request_latency')
		.groupBy('dc')
	|combine(lambda: "service" == 'auth', lambda: TRUE)
		.as('auth', 'other')
		.tolerance(1s)
		.delimiter('.')
	|groupBy('other.service','dc')
	|eval(lambda: "auth.value" / "other.value")
		.as('ratio')
    |httpOut('TestStream_Combine')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "other.service": "log", "auth.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.0 / 6.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "other.service": "cart", "auth.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.0 / 8.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "other.service": "log", "auth.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.5 / 6.5,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "other.service": "cart", "auth.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.5 / 8.5,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Combine", script, 13*time.Second, er, true, nil)
}

func TestStream_Combine_All_Triples(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('request_latency')
		.groupBy('dc')
	|combine(lambda: TRUE, lambda: TRUE, lambda: TRUE)
		.as('first', 'second', 'third')
		.tolerance(1s)
		.delimiter('.')
	|groupBy('first.service', 'second.service', 'third.service', 'dc')
	|eval(lambda: "first.value" + "second.value" + "third.value")
		.as('sum')
    |httpOut('TestStream_Combine')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "first.service": "auth", "second.service": "log", "third.service": "cart"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					2100.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "first.service": "auth", "second.service": "log", "third.service": "cart"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					2250.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Combine", script, 13*time.Second, er, true, nil)
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

	testStreamerWithOutput(t, "TestStream_Join", script, 13*time.Second, er, true, nil)
}

func TestStream_Join_Delimiter(t *testing.T) {

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
		.delimiter('#')
		.streamName('error_view')
	|eval(lambda: "errors#sum" / "views#sum")
		.as('error_percent')
		.keep()
	|httpOut('TestStream_Join')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "cartA"},
				Columns: []string{"time", "error_percent", "errors#sum", "views#sum"},
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
				Columns: []string{"time", "error_percent", "errors#sum", "views#sum"},
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
				Columns: []string{"time", "error_percent", "errors#sum", "views#sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.01,
					32.0,
					3200.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Join", script, 13*time.Second, er, true, nil)
}
func TestStream_Join_DelimiterEmpty(t *testing.T) {

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
		.delimiter('')
		.streamName('error_view')
	|eval(lambda: "errorssum" / "viewssum")
		.as('error_percent')
		.keep()
	|httpOut('TestStream_Join')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "cartA"},
				Columns: []string{"time", "error_percent", "errorssum", "viewssum"},
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
				Columns: []string{"time", "error_percent", "errorssum", "viewssum"},
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
				Columns: []string{"time", "error_percent", "errorssum", "viewssum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.01,
					32.0,
					3200.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Join", script, 13*time.Second, er, true, nil)
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

	testStreamerWithOutput(t, "TestStream_JoinTolerance", script, 13*time.Second, er, true, nil)
}

func TestStream_Join_Fill_Null(t *testing.T) {
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
		.fill('null')
		.streamName('error_view')
	|default()
		.field('errors.value', 0.0)
		.field('views.value', 0.0)
	|eval(lambda:  "errors.value" + "views.value")
		.as('error_percent')
	|window()
		.period(10s)
		.every(10s)
	|count('error_percent')
	|httpOut('TestStream_Join_Fill')
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

	testStreamerWithOutput(t, "TestStream_Join_Fill", script, 13*time.Second, er, true, nil)
}

func TestStream_Join_Fill_Num(t *testing.T) {
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
	|httpOut('TestStream_Join_Fill')
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

	testStreamerWithOutput(t, "TestStream_Join_Fill", script, 13*time.Second, er, true, nil)
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

	testStreamerWithOutput(t, "TestStream_JoinN", script, 15*time.Second, er, false, nil)
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

	testStreamerWithOutput(t, "TestStream_JoinOn", script, 13*time.Second, er, true, nil)
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

	testStreamerWithOutput(t, "TestStream_JoinOn", script, 13*time.Second, er, true, nil)
}

func TestStream_JoinOn_AcrossMeasurement(t *testing.T) {
	var script = `
var building = stream
    |from()
        .measurement('building_power')
        .groupBy('building')

var floor = stream
    |from()
        .measurement('floor_power')
        .groupBy('building', 'floor')

building
    |join(floor)
        .as('building', 'floor')
        .on('building')
        .streamName('power_floor_percentage')
    |eval(lambda: "floor.value" / "building.value")
        .as('value')
    |httpOut('TestStream_JoinOn_AcrossMeasurement')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "power_floor_percentage",
				Tags:    map[string]string{"building": "shack", "floor": "1"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.0 / 30.0,
				}},
			},
			{
				Name:    "power_floor_percentage",
				Tags:    map[string]string{"building": "shack", "floor": "2"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					11.0 / 30.0,
				}},
			},
			{
				Name:    "power_floor_percentage",
				Tags:    map[string]string{"building": "shack", "floor": "3"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					12.0 / 30.0,
				}},
			},
			{
				Name:    "power_floor_percentage",
				Tags:    map[string]string{"building": "hut", "floor": "1"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					19.0 / 40.0,
				}},
			},
			{
				Name:    "power_floor_percentage",
				Tags:    map[string]string{"building": "hut", "floor": "2"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					21.0 / 40.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_JoinOn_AcrossMeasurement", script, 13*time.Second, er, true, nil)
}

func TestStream_JoinOn_Fill_Num(t *testing.T) {
	var script = `
var maintlock = stream
    |from()
        .measurement('maintlock')
        .groupBy('host')

stream
    |from()
        .measurement('disk')
        .groupBy('host', 'path')
    |join(maintlock)
        .as('disk', 'maintlock')
        .on('host')
        .fill(0.0)
        .tolerance(1s)
        .streamName('disk')
    |default()
        .field('maintlock.count', 0)
    |window()
        .period(4s)
        .every(4s)
    |httpOut('TestStream_JoinOn_Fill')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "disk",
				Tags:    map[string]string{"host": "A", "path": "/"},
				Columns: []string{"time", "disk.used_percent", "maintlock.count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						50.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						60.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						70.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						80.0,
						1.0,
					},
				},
			},
			{
				Name:    "disk",
				Tags:    map[string]string{"host": "A", "path": "/tmp"},
				Columns: []string{"time", "disk.used_percent", "maintlock.count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						40.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						30.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						20.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						10.0,
						1.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_JoinOn_Fill", script, 13*time.Second, er, true, nil)
}

func TestStream_JoinOn_Fill_Null(t *testing.T) {
	var script = `
var maintlock = stream
    |from()
        .measurement('maintlock')
        .groupBy('host')

stream
    |from()
        .measurement('disk')
        .groupBy('host', 'path')
    |join(maintlock)
        .as('disk', 'maintlock')
        .on('host')
        .fill('null')
        .tolerance(1s)
        .streamName('disk')
    |default()
        .field('maintlock.count', 0)
        .field('disk.used_percent', 0)
    |window()
        .period(4s)
        .every(4s)
    |httpOut('TestStream_JoinOn_Fill')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "disk",
				Tags:    map[string]string{"host": "A", "path": "/"},
				Columns: []string{"time", "disk.used_percent", "maintlock.count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						50.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						60.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						70.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						80.0,
						1.0,
					},
				},
			},
			{
				Name:    "disk",
				Tags:    map[string]string{"host": "A", "path": "/tmp"},
				Columns: []string{"time", "disk.used_percent", "maintlock.count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						40.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						30.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						20.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						10.0,
						1.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_JoinOn_Fill", script, 13*time.Second, er, true, nil)
}

func TestStream_Union(t *testing.T) {

	var script = `
var cpuT = stream
	|from()
		.measurement('cpu')
		.where(lambda: "cpu" == 'total')
var cpu0 = stream
	|from()
		.measurement('cpu')
		.where(lambda: "cpu" == '0')
var cpu1 = stream
	|from()
		.measurement('cpu')
		.where(lambda: "cpu" == '1')

cpuT
	|union(cpu0, cpu1)
		.rename('cpu_all')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|httpOut('TestStream_Union')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_all",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					20.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Union", script, 15*time.Second, er, false, nil)
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
		.groupBy('host')
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
						Tags:    models.Tags{"host": "serverA"},
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
						Tags:    models.Tags{"host": "serverA"},
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
						Tags:    models.Tags{"host": "serverA"},
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
						Tags:    models.Tags{"host": "serverA"},
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
						Tags:    models.Tags{"host": "serverA"},
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
			Method: "mode",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "mode"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							95.0,
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
						Tags:    models.Tags{"host": "serverA"},
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
						Tags:    models.Tags{"host": "serverA"},
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
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "top", "type"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
								98.0,
								"idle",
							},
							{
								time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
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
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "top", "type"},
						Values: [][]interface{}{
							{
								endTime,
								98.0,
								"idle",
							},
							{
								endTime,
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
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "bottom", "type"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
								91.0,
								"idle",
							},
							{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
								92.0,
								"idle",
							},
							{
								time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
								92.0,
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
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "bottom", "type"},
						Values: [][]interface{}{
							{
								endTime,
								91.0,
								"idle",
							},
							{
								endTime,
								92.0,
								"idle",
							},
							{
								endTime,
								92.0,
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
			false,
			nil,
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
		.groupBy('host')
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
						Tags:    models.Tags{"host": "serverA"},
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
						Tags:    models.Tags{"host": "serverA"},
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
						Tags:    models.Tags{"host": "serverA"},
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
						Tags:    models.Tags{"host": "serverA"},
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
						Tags:    models.Tags{"host": "serverA"},
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
			Method: "mode",
			ER: kapacitor.Result{
				Series: imodels.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "mode"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							95.0,
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
						Tags:    models.Tags{"host": "serverA"},
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
						Tags:    models.Tags{"host": "serverA"},
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
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "top", "type"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
								98.0,
								"idle",
							},
							{
								time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
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
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "top", "type"},
						Values: [][]interface{}{
							{
								endTime,
								98.0,
								"idle",
							},
							{
								endTime,
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
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "bottom", "type"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
								91.0,
								"idle",
							},
							{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
								92.0,
								"idle",
							},
							{
								time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
								92.0,
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
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "bottom", "type"},
						Values: [][]interface{}{
							{
								endTime,
								91.0,
								"idle",
							},
							{
								endTime,
								92.0,
								"idle",
							},
							{
								endTime,
								92.0,
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
			false,
			nil,
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
		.groupBy('host')
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
						Tags:    models.Tags{"host": "serverA"},
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
						Tags:    models.Tags{"host": "serverA"},
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
			false,
			nil,
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
		.groupBy('host')
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
						Tags:    models.Tags{"host": "serverA"},
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
						Tags:    models.Tags{"host": "serverA"},
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
			false,
			nil,
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

	tmInit := func(tm *kapacitor.TaskMaster) {
		tm.UDFService = udfService
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

	testStreamerWithOutput(t, "TestStream_CustomFunctions", script, 15*time.Second, er, false, tmInit)
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
			Level:   alert.Critical,
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
		.messageField('msg')
		.levelTag('level')
		.info(lambda: "count" > infoThreshold)
		.warn(lambda: "count" > warnThreshold)
		.crit(lambda: "count" > critThreshold)
		.post('` + ts.URL + `')
	|httpOut('TestStream_Alert')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA", "level": "CRITICAL", "id": "kapacitor/cpu/serverA"},
				Columns: []string{"time", "count", "id", "level", "msg"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
					"kapacitor/cpu/serverA",
					"CRITICAL",
					"kapacitor/cpu/serverA is CRITICAL",
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Alert", script, 13*time.Second, er, false, nil)

	if rc := atomic.LoadInt32(&requestCount); rc != 1 {
		t.Errorf("got %v exp %v", rc, 1)
	}
}

func TestStream_Alert_NoRecoveries(t *testing.T) {
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
		var expAd kapacitor.AlertData
		switch rc {
		case 1:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is WARNING",
				Time:     time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
				Duration: 0,
				Level:    alert.Warning,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
								7.0,
							}},
						},
					},
				},
			}
		case 2:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is INFO",
				Time:     time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
				Duration: 0,
				Level:    alert.Info,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
								6.0,
							}},
						},
					},
				},
			}
		case 3:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is WARNING",
				Time:     time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
				Duration: time.Second,
				Level:    alert.Warning,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
								7.0,
							}},
						},
					},
				},
			}
		case 4:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is WARNING",
				Time:     time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
				Duration: 2 * time.Second,
				Level:    alert.Warning,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
								7.0,
							}},
						},
					},
				},
			}
		case 5:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is CRITICAL",
				Time:     time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
				Duration: 3 * time.Second,
				Level:    alert.Critical,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
								8.0,
							}},
						},
					},
				},
			}
		case 6:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is INFO",
				Time:     time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
				Duration: 0,
				Level:    alert.Info,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
								6.0,
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
var infoThreshold = 6.0
var warnThreshold = 7.0
var critThreshold = 8.0

stream
	|from()
		.measurement('cpu')
		.groupBy('host')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.details('')
		.info(lambda: "value" >= infoThreshold)
		.warn(lambda: "value" >= warnThreshold)
		.crit(lambda: "value" >= critThreshold)
		.noRecoveries()
		.post('` + ts.URL + `')
	|httpOut('TestStream_Alert_NoRecoveries')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
					6.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Alert_NoRecoveries", script, 13*time.Second, er, false, nil)

	if rc, exp := atomic.LoadInt32(&requestCount), int32(6); rc != exp {
		t.Errorf("unexpected requestCount: got %v exp %v", rc, exp)
	}
}

func TestStream_Alert_WithReset_0(t *testing.T) {
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
		var expAd kapacitor.AlertData
		switch rc {
		case 1:
			expAd = kapacitor.AlertData{
				ID:      "kapacitor/cpu/serverA",
				Message: "kapacitor/cpu/serverA is INFO",
				Details: "details",
				Time:    time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
				Level:   alert.Info,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
								45.0,
							}},
						},
					},
				},
			}
		case 2:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is INFO",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
				Duration: time.Second,
				Level:    alert.Info,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
								40.0,
							}},
						},
					},
				},
			}
		case 3:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is INFO",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
				Duration: 2 * time.Second,
				Level:    alert.Info,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
								30.0,
							}},
						},
					},
				},
			}
		case 4:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is OK",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
				Duration: 3 * time.Second,
				Level:    alert.OK,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
								9.0,
							}},
						},
					},
				},
			}
		case 5:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is INFO",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
				Duration: 0 * time.Second,
				Level:    alert.Info,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
								45.0,
							}},
						},
					},
				},
			}
		case 6:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is WARNING",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
				Duration: 1 * time.Second,
				Level:    alert.Warning,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
								61.0,
							}},
						},
					},
				},
			}
		case 7:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is WARNING",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
				Duration: 2 * time.Second,
				Level:    alert.Warning,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
								30.0,
							}},
						},
					},
				},
			}
		case 8:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is OK",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
				Duration: 3 * time.Second,
				Level:    alert.OK,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
								19.0,
							}},
						},
					},
				},
			}
		case 9:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is INFO",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
				Duration: 0 * time.Second,
				Level:    alert.Info,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
								45.0,
							}},
						},
					},
				},
			}
		case 10:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is WARNING",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
				Duration: 1 * time.Second,
				Level:    alert.Warning,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
								61.0,
							}},
						},
					},
				},
			}
		case 11:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is CRITICAL",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
				Duration: 2 * time.Second,
				Level:    alert.Critical,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
								81.0,
							}},
						},
					},
				},
			}
		case 12:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is OK",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
				Duration: 3 * time.Second,
				Level:    alert.OK,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
								29.0,
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
var infoThreshold = 40.0
var warnThreshold = 60.0
var critThreshold = 80.0

var infoResetThreshold = 10.0
var warnResetThreshold = 20.0
var critResetThreshold = 30.0

stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.details('details')
		.idField('id')
		.idTag('id')
		.levelField('level')
		.levelTag('level')
		.info(lambda: "value" > infoThreshold)
		.infoReset(lambda: "value" < infoResetThreshold)
		.warn(lambda: "value" > warnThreshold)
		.warnReset(lambda: "value" < warnResetThreshold)
		.crit(lambda: "value" > critThreshold)
		.critReset(lambda: "value" < critResetThreshold)
		.post('` + ts.URL + `')
	|httpOut('TestStream_Alert_WithReset_0')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA", "level": "OK", "id": "kapacitor/cpu/serverA", "type": "usage"},
				Columns: []string{"time", "id", "level", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
					"kapacitor/cpu/serverA",
					"OK",
					29.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Alert_WithReset_0", script, 13*time.Second, er, false, nil)

	if rc := atomic.LoadInt32(&requestCount); rc != 12 {
		t.Errorf("got %v exp %v", rc, 1)
	}
}

func TestStream_Alert_WithReset_1(t *testing.T) {
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
		var expAd kapacitor.AlertData
		switch rc {
		case 1:
			expAd = kapacitor.AlertData{
				ID:      "kapacitor/cpu/serverA",
				Message: "kapacitor/cpu/serverA is INFO",
				Details: "details",
				Time:    time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
				Level:   alert.Info,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
								45.0,
							}},
						},
					},
				},
			}
		case 2:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is INFO",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
				Duration: time.Second,
				Level:    alert.Info,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
								40.0,
							}},
						},
					},
				},
			}
		case 3:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is INFO",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
				Duration: 2 * time.Second,
				Level:    alert.Info,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
								30.0,
							}},
						},
					},
				},
			}
		case 4:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is OK",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
				Duration: 3 * time.Second,
				Level:    alert.OK,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
								29.0,
							}},
						},
					},
				},
			}
		case 5:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is INFO",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
				Duration: 0 * time.Second,
				Level:    alert.Info,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
								45.0,
							}},
						},
					},
				},
			}
		case 6:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is WARNING",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
				Duration: 1 * time.Second,
				Level:    alert.Warning,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
								61.0,
							}},
						},
					},
				},
			}
		case 7:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is INFO",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
				Duration: 2 * time.Second,
				Level:    alert.Info,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
								49.0,
							}},
						},
					},
				},
			}
		case 8:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is OK",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
				Duration: 3 * time.Second,
				Level:    alert.OK,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
								29.0,
							}},
						},
					},
				},
			}
		case 9:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is INFO",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
				Duration: 0 * time.Second,
				Level:    alert.Info,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
								45.0,
							}},
						},
					},
				},
			}
		case 10:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is WARNING",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
				Duration: 1 * time.Second,
				Level:    alert.Warning,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
								61.0,
							}},
						},
					},
				},
			}
		case 11:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is CRITICAL",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
				Duration: 2 * time.Second,
				Level:    alert.Critical,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
								81.0,
							}},
						},
					},
				},
			}
		case 12:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is WARNING",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
				Duration: 3 * time.Second,
				Level:    alert.Warning,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
								69.0,
							}},
						},
					},
				},
			}
		case 13:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is WARNING",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 12, 0, time.UTC),
				Duration: 4 * time.Second,
				Level:    alert.Warning,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 12, 0, time.UTC),
								50.0,
							}},
						},
					},
				},
			}
		case 14:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is INFO",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 13, 0, time.UTC),
				Duration: 5 * time.Second,
				Level:    alert.Info,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 13, 0, time.UTC),
								41.0,
							}},
						},
					},
				},
			}
		case 15:
			expAd = kapacitor.AlertData{
				ID:       "kapacitor/cpu/serverA",
				Message:  "kapacitor/cpu/serverA is OK",
				Details:  "details",
				Time:     time.Date(1971, 1, 1, 0, 0, 14, 0, time.UTC),
				Duration: 6 * time.Second,
				Level:    alert.OK,
				Data: influxql.Result{
					Series: imodels.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 14, 0, time.UTC),
								25.0,
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
var infoThreshold = 40.0
var warnThreshold = 60.0
var critThreshold = 80.0

var infoResetThreshold = 30.0
var warnResetThreshold = 50.0
var critResetThreshold = 70.0

stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.details('details')
		.idField('id')
		.idTag('id')
		.levelField('level')
		.levelTag('level')
		.info(lambda: "value" > infoThreshold)
		.infoReset(lambda: "value" < infoResetThreshold)
		.warn(lambda: "value" > warnThreshold)
		.warnReset(lambda: "value" < warnResetThreshold)
		.crit(lambda: "value" > critThreshold)
		.critReset(lambda: "value" < critResetThreshold)
		.post('` + ts.URL + `')
	|httpOut('TestStream_Alert_WithReset_1')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA", "level": "OK", "id": "kapacitor/cpu/serverA", "type": "usage"},
				Columns: []string{"time", "id", "level", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 14, 0, time.UTC),
					"kapacitor/cpu/serverA",
					"OK",
					25.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Alert_WithReset_1", script, 15*time.Second, er, false, nil)

	if rc := atomic.LoadInt32(&requestCount); rc != 15 {
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
				Level:    alert.Critical,
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
				Level:    alert.Warning,
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
				Level:    alert.OK,
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
				Level:    alert.Warning,
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
				Level:    alert.OK,
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

	testStreamerWithOutput(t, "TestStream_AlertDuration", script, 13*time.Second, er, false, nil)

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
	tmInit := func(tm *kapacitor.TaskMaster) {
		c := sensu.NewConfig()
		c.Enabled = true
		c.Addr = listen.Addr().String()
		c.Source = "Kapacitor"
		sl := sensu.NewService(c, logService.NewLogger("[test_sensu] ", log.LstdFlags))
		tm.SensuService = sl
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

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

	tmInit := func(tm *kapacitor.TaskMaster) {
		c := slack.NewConfig()
		c.Enabled = true
		c.URL = ts.URL + "/test/slack/url"
		c.Channel = "#channel"
		sl := slack.NewService(c, logService.NewLogger("[test_slack] ", log.LstdFlags))
		tm.SlackService = sl
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	if rc := atomic.LoadInt32(&requestCount); rc != 2 {
		t.Errorf("unexpected requestCount got %d exp 2", rc)
	}
}

func TestStream_AlertTelegram(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		type postData struct {
			ChatId                string `json:"chat_id"`
			Text                  string `json:"text"`
			ParseMode             string `json:"parse_mode"`
			DisableWebPagePreview bool   `json:"disable_web_page_preview"`
			DisableNotification   bool   `json:"disable_notification"`
		}
		pd := postData{}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&pd)

		if exp := "/botTOKEN:AUTH/sendMessage"; r.URL.String() != exp {
			t.Errorf("unexpected url got %s exp %s", r.URL.String(), exp)
		}

		if rc := atomic.LoadInt32(&requestCount); rc == 1 {
			if exp := "12345678"; pd.ChatId != exp {
				t.Errorf("unexpected recipient got %s exp %s", pd.ChatId, exp)
			}
			if exp := "HTML"; pd.ParseMode != exp {
				t.Errorf("unexpected recipient got %s exp %s", pd.ParseMode, exp)
			}
			if exp := true; pd.DisableWebPagePreview != exp {
				t.Errorf("unexpected DisableWebPagePreview got %t exp %t", pd.DisableWebPagePreview, exp)
			}
			if exp := true; pd.DisableNotification != exp {
				t.Errorf("unexpected DisableNotification got %t exp %t", pd.DisableNotification, exp)
			}

		} else if rc := atomic.LoadInt32(&requestCount); rc == 2 {
			if exp := "87654321"; pd.ChatId != exp {
				t.Errorf("unexpected recipient got %s exp %s", pd.ChatId, exp)
			}
			if exp := ""; pd.ParseMode != exp {
				t.Errorf("unexpected recipient got '%s' exp '%s'", pd.ParseMode, exp)
			}
			if exp := true; pd.DisableWebPagePreview != exp {
				t.Errorf("unexpected DisableWebPagePreview got %t exp %t", pd.DisableWebPagePreview, exp)
			}
			if exp := false; pd.DisableNotification != exp {
				t.Errorf("unexpected DisableNotification got %t exp %t", pd.DisableNotification, exp)
			}
		}

		if exp := "kapacitor/cpu/serverA is CRITICAL"; pd.Text != exp {
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
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.telegram()
			.chatId('12345678')
                	.disableNotification()
                	.parseMode('HTML')
                .telegram()
                	.chatId('87654321')
`
	tmInit := func(tm *kapacitor.TaskMaster) {
		c := telegram.NewConfig()
		c.Enabled = true
		c.URL = ts.URL + "/bot"
		c.Token = "TOKEN:AUTH"
		c.ChatId = "123456789"
		c.DisableWebPagePreview = true
		c.DisableNotification = false
		tl := telegram.NewService(c, logService.NewLogger("[test_telegram] ", log.LstdFlags))
		tm.TelegramService = tl
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

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
	tmInit := func(tm *kapacitor.TaskMaster) {

		c := hipchat.NewConfig()
		c.Enabled = true
		c.URL = ts.URL
		c.Room = "1231234"
		c.Token = "testtoken1231234"
		sl := hipchat.NewService(c, logService.NewLogger("[test_hipchat] ", log.LstdFlags))
		tm.HipChatService = sl
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

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
			if exp := "/alert"; r.URL.String() != exp {
				t.Errorf("unexpected url got %s exp %s", r.URL.String(), exp)
			}
			if exp := "Bearer testtoken1234567"; r.Header.Get("Authorization") != exp {
				t.Errorf("unexpected token in header got %s exp %s", r.Header.Get("Authorization"), exp)
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
			if exp := "host=serverA"; pd.Group != exp {
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
			if exp := "/alert"; r.URL.String() != exp {
				t.Errorf("unexpected url got %s exp %s", r.URL.String(), exp)
			}
			if exp := "Bearer anothertesttoken"; r.Header.Get("Authorization") != exp {
				t.Errorf("unexpected token in header got %s exp %s", r.Header.Get("Authorization"), exp)
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
		w.WriteHeader(http.StatusCreated)
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
	tmInit := func(tm *kapacitor.TaskMaster) {
		c := alerta.NewConfig()
		c.Enabled = true
		c.URL = ts.URL
		c.Origin = "Kapacitor"
		sl := alerta.NewService(c, logService.NewLogger("[test_alerta] ", log.LstdFlags))
		tm.AlertaService = sl
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

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
	tmInit := func(tm *kapacitor.TaskMaster) {
		c := opsgenie.NewConfig()
		c.Enabled = true
		c.URL = ts.URL
		c.APIKey = "api_key"
		og := opsgenie.NewService(c, logService.NewLogger("[test_og] ", log.LstdFlags))
		tm.OpsGenieService = og
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

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

	tmInit := func(tm *kapacitor.TaskMaster) {
		c := pagerduty.NewConfig()
		c.Enabled = true
		c.URL = ts.URL
		c.ServiceKey = "service_key"
		pd := pagerduty.NewService(c, logService.NewLogger("[test_pd] ", log.LstdFlags))
		pd.HTTPDService = tm.HTTPDService
		tm.PagerDutyService = pd
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

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

	tmInit := func(tm *kapacitor.TaskMaster) {
		c := victorops.NewConfig()
		c.Enabled = true
		c.URL = ts.URL
		c.APIKey = "api_key"
		c.RoutingKey = "routing_key"
		vo := victorops.NewService(c, logService.NewLogger("[test_vo] ", log.LstdFlags))
		tm.VictorOpsService = vo
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	if got, exp := atomic.LoadInt32(&requestCount), int32(2); got != exp {
		t.Errorf("unexpected requestCount got %d exp %d", got, exp)
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

	tmInit := func(tm *kapacitor.TaskMaster) {
		c := talk.NewConfig()
		c.Enabled = true
		c.URL = ts.URL
		c.AuthorName = "Kapacitor"
		sl := talk.NewService(c, logService.NewLogger("[test_talk] ", log.LstdFlags))
		tm.TalkService = sl
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	if rc := atomic.LoadInt32(&requestCount); rc != 1 {
		t.Errorf("unexpected requestCount got %d exp 1", rc)
	}
}

func TestStream_AlertLog(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "TestStream_AlertLog")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	normalPath := filepath.Join(tmpDir, "normal.log")
	modePath := filepath.Join(tmpDir, "mode.log")
	var script = fmt.Sprintf(`
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
		.details('')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.log('%s')
		.log('%s')
			.mode(0644)
`, normalPath, modePath)

	expAD := kapacitor.AlertData{
		ID:      "kapacitor.cpu.serverA",
		Message: "kapacitor.cpu.serverA is CRITICAL",
		Time:    time.Date(1971, 01, 01, 0, 0, 10, 0, time.UTC),
		Level:   alert.Critical,
		Data: influxql.Result{
			Series: imodels.Rows{
				{
					Name:    "cpu",
					Tags:    map[string]string{"host": "serverA"},
					Columns: []string{"time", "count"},
					Values: [][]interface{}{[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC).Format(time.RFC3339Nano),
						10.0,
					}},
				},
			},
		},
	}

	testAD := func(name string, f io.Reader) {
		ad := kapacitor.AlertData{}
		if err := json.NewDecoder(f).Decode(&ad); err != nil {
			t.Fatal(err)
		}
		if got, exp := ad, expAD; !reflect.DeepEqual(got, exp) {
			t.Errorf("%s unexpected alert data written to log:\ngot\n%+v\nexp\n%+v\n", name, got, exp)
		}
	}

	clock, et, replayErr, tm := testStreamer(t, "TestStream_Alert", script, nil)
	defer tm.Close()

	err = fastForwardTask(clock, et, replayErr, tm, 13*time.Second)
	if err != nil {
		t.Error(err)
	}

	normal, err := os.Open(normalPath)
	if err != nil {
		t.Fatalf("missing log file for alert %v", err)
	}
	defer normal.Close()
	if stat, err := normal.Stat(); err != nil {
		t.Fatal(err)
	} else if exp, got := os.FileMode(0600), stat.Mode(); exp != got {
		t.Errorf("unexpected normal file mode: got %v exp %v", got, exp)
	}
	testAD("normal", normal)

	mode, err := os.Open(modePath)
	if err != nil {
		t.Fatalf("missing log file for alert %v", err)
	}
	defer mode.Close()
	if stat, err := mode.Stat(); err != nil {
		t.Fatal(err)
	} else if exp, got := os.FileMode(0644), stat.Mode(); exp != got {
		t.Errorf("unexpected normal file mode: got %v exp %v", got, exp)
	}

	testAD("mode", mode)
}

func TestStream_AlertExec(t *testing.T) {
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
		.details('')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.exec('/bin/my-script', 'arg1', 'arg2')
		.exec('/bin/my-other-script')
`

	expInfo := []command.CommandInfo{
		{
			Prog: "/bin/my-script",
			Args: []string{"arg1", "arg2"},
		},
		{
			Prog: "/bin/my-other-script",
			Args: []string{},
		},
	}
	expAD := kapacitor.AlertData{
		ID:      "kapacitor.cpu.serverA",
		Message: "kapacitor.cpu.serverA is CRITICAL",
		Time:    time.Date(1971, 01, 01, 0, 0, 10, 0, time.UTC),
		Level:   alert.Critical,
		Data: influxql.Result{
			Series: imodels.Rows{
				{
					Name:    "cpu",
					Tags:    map[string]string{"host": "serverA"},
					Columns: []string{"time", "count"},
					Values: [][]interface{}{[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC).Format(time.RFC3339Nano),
						10.0,
					}},
				},
			},
		},
	}

	cmdC := make(chan *commandtest.CommandTest, 2)
	tmInit := func(tm *kapacitor.TaskMaster) {
		tm.Commander = commandtest.CommanderTest{
			NewCommandHook: func(c *commandtest.CommandTest) {
				log.Println("D! command!")
				cmdC <- c
			},
		}
	}

	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)
	for i := 0; i < 2; i++ {
		select {
		case cmd := <-cmdC:
			cmd.Lock()
			defer cmd.Unlock()

			if got, exp := cmd.Info, expInfo[i]; !reflect.DeepEqual(got, exp) {
				t.Errorf("%d unexpected command info:\ngot\n%+v\nexp\n%+v\n", i, got, exp)
			}

			if !cmd.Started {
				t.Errorf("%d expected command to have been started", i)
			}
			if !cmd.Waited {
				t.Errorf("%d expected command to have waited", i)
			}
			if cmd.Killed {
				t.Errorf("%d expected command not to have been killed", i)
			}

			ad := kapacitor.AlertData{}
			err := json.Unmarshal(cmd.StdinData, &ad)
			if err != nil {
				t.Fatal(err)
			}
			if got, exp := ad, expAD; !reflect.DeepEqual(got, exp) {
				t.Errorf("%d unexpected alert data sent to command:\ngot\n%+v\nexp\n%+v\n%s", i, got, exp, string(cmd.StdinData))
			}
		default:
			t.Error("expected command to be created")
		}
	}
}

func TestStream_AlertEmail(t *testing.T) {
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
		.details('''
<b>{{.Message}}</b>

Value: {{ index .Fields "count" }}
<a href="http://graphs.example.com/host/{{index .Tags "host"}}">Details</a>
''')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.email('user1@example.com', 'user2@example.com')
		.email()
			.to('user1@example.com', 'user2@example.com')
`

	expMail := []*smtptest.Message{
		{
			Header: mail.Header{
				"Mime-Version":              []string{"1.0"},
				"Content-Type":              []string{"text/html; charset=UTF-8"},
				"Content-Transfer-Encoding": []string{"quoted-printable"},
				"To":      []string{"user1@example.com, user2@example.com"},
				"From":    []string{"test@example.com"},
				"Subject": []string{"kapacitor.cpu.serverA is CRITICAL"},
			},
			Body: `
<b>kapacitor.cpu.serverA is CRITICAL</b>

Value: 10
<a href=3D"http://graphs.example.com/host/serverA">Details</a>
`,
		},
		{
			Header: mail.Header{
				"Mime-Version":              []string{"1.0"},
				"Content-Type":              []string{"text/html; charset=UTF-8"},
				"Content-Transfer-Encoding": []string{"quoted-printable"},
				"To":      []string{"user1@example.com, user2@example.com"},
				"From":    []string{"test@example.com"},
				"Subject": []string{"kapacitor.cpu.serverA is CRITICAL"},
			},
			Body: `
<b>kapacitor.cpu.serverA is CRITICAL</b>

Value: 10
<a href=3D"http://graphs.example.com/host/serverA">Details</a>
`,
		},
	}
	compareMessages := func(exp, got *smtptest.Message) error {
		if exp.Body != got.Body {
			return fmt.Errorf("unequal bodies:\ngot\n%q\nexp\n%q\n", got.Body, exp.Body)
		}
		// Compare only the header keys specified in the exp message.
		for k, ev := range exp.Header {
			gv, ok := got.Header[k]
			if !ok {
				return fmt.Errorf("missing header %s", k)
			}
			if len(gv) != len(ev) {
				return fmt.Errorf("unexpected header %s: got %v exp %v", k, gv, ev)
			}
			for i := range ev {
				if gv[i] != ev[i] {
					return fmt.Errorf("unexpected header %s: got %v exp %v", k, gv, ev)
				}
			}
		}
		return nil
	}

	smtpServer, err := smtptest.NewServer()
	if err != nil {
		t.Fatal(err)
	}
	defer smtpServer.Close()
	sc := smtp.Config{
		Enabled: true,
		Host:    smtpServer.Host,
		Port:    smtpServer.Port,
		From:    "test@example.com",
	}
	smtpService := smtp.NewService(sc, logService.NewLogger("[test-smtp] ", log.LstdFlags))
	if err := smtpService.Open(); err != nil {
		t.Fatal(err)
	}
	defer smtpService.Close()

	tmInit := func(tm *kapacitor.TaskMaster) {
		tm.SMTPService = smtpService
	}

	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	smtpServer.Close()

	errors := smtpServer.Errors()
	if got, exp := len(errors), 0; got != exp {
		t.Errorf("unexpected smtp server errors: %v", errors)
	}

	msgs := smtpServer.SentMessages()
	if got, exp := len(msgs), len(expMail); got != exp {
		t.Errorf("unexpected number of messages sent: got %d exp %d", got, exp)
	}
	for i, exp := range expMail {
		got := msgs[i]
		if err := compareMessages(exp, got); err != nil {
			t.Errorf("%d %s", i, err)
		}
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
				Level:   alert.Info,
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
				Level:    alert.OK,
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

	testStreamerNoOutput(t, "TestStream_AlertSigma", script, 13*time.Second, nil)

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
			Level:   alert.Critical,
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

	testStreamerNoOutput(t, "TestStream_AlertComplexWhere", script, 13*time.Second, nil)

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

	testStreamerNoOutput(t, "TestStream_AlertStateChangesOnly", script, 13*time.Second, nil)

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
				Level:    alert.Critical,
			}
		} else {
			expAd = kapacitor.AlertData{
				ID:       "cpu:nil",
				Message:  "cpu:nil is OK",
				Time:     time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
				Duration: 9 * time.Second,
				Level:    alert.OK,
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

	testStreamerNoOutput(t, "TestStream_AlertStateChangesOnlyExpired", script, 13*time.Second, nil)

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

	testStreamerNoOutput(t, "TestStream_AlertFlapping", script, 13*time.Second, nil)

	// Flapping detection should drop the last alerts.
	if rc := atomic.LoadInt32(&requestCount); rc != 9 {
		t.Errorf("got %v exp %v", rc, 9)
	}
}

func TestStream_K8sAutoscale(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('scale')
		.groupBy('deployment')
	|k8sAutoscale()
		.resourceNameTag('deployment')
		.replicas(lambda: int("replicas"))
	|httpOut('TestStream_K8sAutoscale')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name: "scale",
				Tags: map[string]string{
					"deployment": "serviceA",
					"namespace":  "default",
					"kind":       "deployments",
					"resource":   "serviceA",
				},
				Columns: []string{"time", "new", "old"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
					2.0,
					1000.0,
				}},
			},
			{
				Name: "scale",
				Tags: map[string]string{
					"deployment": "serviceB",
					"namespace":  "default",
					"kind":       "deployments",
					"resource":   "serviceB",
				},
				Columns: []string{"time", "new", "old"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
					20.0,
					1000.0,
				}},
			},
		},
	}
	scaleUpdates := make(chan k8s.Scale, 100)
	k8sAutoscale := k8sAutoscale{}
	k8sAutoscale.ScalesGetFunc = func(kind, name string) (*k8s.Scale, error) {
		var replicas int32
		switch name {
		case "serviceA":
			replicas = 1
		case "serviceB":
			replicas = 10
		}
		return &k8s.Scale{
			ObjectMeta: k8s.ObjectMeta{
				Name: name,
			},
			Spec: k8s.ScaleSpec{
				Replicas: replicas,
			},
		}, nil
	}
	k8sAutoscale.ScalesUpdateFunc = func(kind string, scale *k8s.Scale) error {
		scaleUpdates <- *scale
		return nil
	}
	tmInit := func(tm *kapacitor.TaskMaster) {
		tm.K8sService = k8sAutoscale
	}

	testStreamerWithOutput(t, "TestStream_K8sAutoscale", script, 13*time.Second, er, false, tmInit)

	close(scaleUpdates)
	updatesByService := make(map[string][]int32)
	for scale := range scaleUpdates {
		updatesByService[scale.Name] = append(updatesByService[scale.Name], scale.Spec.Replicas)
	}
	expUpdates := map[string][]int32{
		"serviceA": []int32{2, 1, 1000, 2},
		"serviceB": []int32{20, 1, 1000, 20},
	}

	if !reflect.DeepEqual(updatesByService, expUpdates) {
		t.Errorf("unexpected updates\ngot\n%v\nexp\n%v\n", updatesByService, expUpdates)
	}
}
func TestStream_K8sAutoscale_MinMax(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('scale')
		.groupBy('deployment')
	|k8sAutoscale()
		.min(3)
		.max(500)
		.resourceNameTag('deployment')
		.replicas(lambda: int("replicas"))
	|httpOut('TestStream_K8sAutoscale')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name: "scale",
				Tags: map[string]string{
					"deployment": "serviceA",
					"namespace":  "default",
					"kind":       "deployments",
					"resource":   "serviceA",
				},
				Columns: []string{"time", "new", "old"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
					3.0,
					500.0,
				}},
			},
			{
				Name: "scale",
				Tags: map[string]string{
					"deployment": "serviceB",
					"namespace":  "default",
					"kind":       "deployments",
					"resource":   "serviceB",
				},
				Columns: []string{"time", "new", "old"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
					20.0,
					500.0,
				}},
			},
		},
	}
	scaleUpdates := make(chan k8s.Scale, 100)
	k8sAutoscale := k8sAutoscale{}
	k8sAutoscale.ScalesGetFunc = func(kind, name string) (*k8s.Scale, error) {
		var replicas int32
		switch name {
		case "serviceA":
			replicas = 1
		case "serviceB":
			replicas = 10
		}
		return &k8s.Scale{
			ObjectMeta: k8s.ObjectMeta{
				Name: name,
			},
			Spec: k8s.ScaleSpec{
				Replicas: replicas,
			},
		}, nil
	}
	k8sAutoscale.ScalesUpdateFunc = func(kind string, scale *k8s.Scale) error {
		scaleUpdates <- *scale
		return nil
	}
	tmInit := func(tm *kapacitor.TaskMaster) {
		tm.K8sService = k8sAutoscale
	}

	testStreamerWithOutput(t, "TestStream_K8sAutoscale", script, 13*time.Second, er, false, tmInit)

	close(scaleUpdates)
	updatesByService := make(map[string][]int32)
	for scale := range scaleUpdates {
		updatesByService[scale.Name] = append(updatesByService[scale.Name], scale.Spec.Replicas)
	}
	expUpdates := map[string][]int32{
		"serviceA": []int32{3, 500, 3},
		"serviceB": []int32{20, 3, 500, 20},
	}

	if !reflect.DeepEqual(updatesByService, expUpdates) {
		t.Errorf("unexpected updates\ngot\n%v\nexp\n%v\n", updatesByService, expUpdates)
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

	tmInit := func(tm *kapacitor.TaskMaster) {
		tm.InfluxDBService = influxdb
	}
	testStreamerNoOutput(t, "TestStream_InfluxDBOut", script, 15*time.Second, tmInit)

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
		if got, exp := p.Tags().GetString("key"), "value"; got != exp {
			t.Errorf("got %s exp %s", got, exp)
		}
		tm := time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC)
		if !tm.Equal(p.Time()) {
			t.Errorf("times are not equal exp %s got %s", tm, p.Time())
		}
	}
}
func TestStream_InfluxDBOut_CreateDatabase(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'nonexistant')
	|influxDBOut()
		.create()
		.database('db')
`

	done := make(chan error, 1)
	var createQuery string

	influxdb := NewMockInfluxDBService(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ping" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		createQuery = r.URL.Query().Get("q")
		done <- nil
	}))

	name := "TestStream_InfluxDBOut"

	// Create a new execution env
	tm := kapacitor.NewTaskMaster("testStreamer", logService)
	tm.HTTPDService = httpService
	tm.TaskStore = taskStore{}
	tm.DeadmanService = deadman{}
	tm.InfluxDBService = influxdb
	tm.Open()

	//Create the task
	task, err := tm.NewTask(name, script, kapacitor.StreamTask, dbrps, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	//Start the task
	et, err := tm.StartTask(task)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(string(et.Task.Dot()))
	defer tm.Close()

	// Wait till we received a request
	if e := <-done; e != nil {
		t.Error(e)
	}

	expCreateQuery := `CREATE DATABASE db`
	if createQuery != expCreateQuery {
		t.Errorf("unexpected create database query: got %q exp: %q", createQuery, expCreateQuery)
	}
}
func TestStream_InfluxDBOut_CreateDatabaseAndRP(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'nonexistant')
	|influxDBOut()
		.create()
		.database('db')
		.retentionPolicy('rp')
`

	done := make(chan error, 1)
	var createQuery string

	influxdb := NewMockInfluxDBService(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ping" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		createQuery = r.URL.Query().Get("q")
		done <- nil
	}))

	name := "TestStream_InfluxDBOut"

	// Create a new execution env
	tm := kapacitor.NewTaskMaster("testStreamer", logService)
	tm.HTTPDService = httpService
	tm.TaskStore = taskStore{}
	tm.DeadmanService = deadman{}
	tm.InfluxDBService = influxdb
	tm.Open()

	//Create the task
	task, err := tm.NewTask(name, script, kapacitor.StreamTask, dbrps, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	//Start the task
	et, err := tm.StartTask(task)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(string(et.Task.Dot()))
	defer tm.Close()

	// Wait till we received a request
	if e := <-done; e != nil {
		t.Error(e)
	}

	expCreateQuery := `CREATE DATABASE db WITH NAME rp`
	if createQuery != expCreateQuery {
		t.Errorf("unexpected create database query: got %q exp: %q", createQuery, expCreateQuery)
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

	testStreamerWithOutput(t, "TestStream_Selectors", script, 15*time.Second, er, false, nil)
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
	tmInit func(tm *kapacitor.TaskMaster),
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
	tm.TaskStore = taskStore{}
	tm.DeadmanService = deadman{}
	tm.AlertService = alert.NewService(alert.NewConfig(), logService.NewLogger("[alert] ", log.LstdFlags))
	if tmInit != nil {
		tmInit(tm)
	}
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
	tmInit func(tm *kapacitor.TaskMaster),
) {
	clock, et, replayErr, tm := testStreamer(t, name, script, tmInit)
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
	ignoreOrder bool,
	tmInit func(tm *kapacitor.TaskMaster),
) {
	clock, et, replayErr, tm := testStreamer(t, name, script, tmInit)
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
