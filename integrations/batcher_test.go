package integrations

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/influxql"
	imodels "github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/clock"
	"github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/wlog"
)

func TestBatch_InvalidQuery(t *testing.T) {

	// Create a new execution env
	tm := kapacitor.NewTaskMaster("invalidQuery", logService)
	tm.HTTPDService = httpService
	tm.TaskStore = taskStore{}
	tm.DeadmanService = deadman{}
	tm.Open()
	defer tm.Close()

	testCases := []struct {
		script string
		err    string
	}{
		{
			script: `batch|query('SELECT value FROM db.rp.m; DROP DATABASE _internal').every(1s)`,
			err:    "query must be a single select statement, got 2 statements",
		},
		{
			script: `batch|query('DROP DATABASE _internal').every(1s)`,
			err:    `query is not a select statement "DROP DATABASE _internal"`,
		},
	}

	for _, tc := range testCases {
		task, err := tm.NewTask("invalid", tc.script, kapacitor.BatchTask, dbrps, 0, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		if _, err := tm.StartTask(task); err == nil {
			t.Errorf("expected error for invalid query %s", task.Dot())
		} else if got := err.Error(); got != tc.err {
			t.Errorf("unexpected error got %s exp %s", got, tc.err)
		}
	}
}

func TestBatch_Derivative(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT sum("value") as "value"
		FROM "telegraf"."default".packets
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))
	|derivative('value')
	|httpOut('TestBatch_Derivative')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						0.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						0.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						0.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						0.5,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Derivative", script, 21*time.Second, er, false)
}

func TestBatch_DerivativeUnit(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT sum("value") as "value"
		FROM "telegraf"."default".packets
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))
	|derivative('value')
		.unit(2s)
	|httpOut('TestBatch_Derivative')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						1.0,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Derivative", script, 21*time.Second, er, false)
}

func TestBatch_DerivativeN(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT sum("value") as "value"
		FROM "telegraf"."default".packets
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))
	|derivative('value')
	|httpOut('TestBatch_DerivativeNN')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						0.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						0.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						-501.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						0.5,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_DerivativeNN", script, 21*time.Second, er, false)
}

func TestBatch_DerivativeNN(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT sum("value") as "value"
		FROM "telegraf"."default".packets
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))
	|derivative('value')
		.nonNegative()
	|httpOut('TestBatch_DerivativeNN')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						0.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						0.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						0.5,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_DerivativeNN", script, 21*time.Second, er, false)
}

func TestBatch_Elapsed(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT "value"
		FROM "telegraf"."default".packets
''')
		.period(10s)
		.every(10s)
	|elapsed('value', 1ms)
	|httpOut('TestBatch_Elapsed')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "elapsed"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						2000.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						2000.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						2000.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						2000.0,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Elapsed", script, 21*time.Second, er, false)
}

func TestBatch_Difference(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT "value"
		FROM "telegraf"."default".packets
''')
		.period(10s)
		.every(10s)
	|difference('value')
	|log()
	|httpOut('TestBatch_Difference')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "difference"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						-5.0,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Difference", script, 21*time.Second, er, false)
}

func TestBatch_MovingAverage(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT "value"
		FROM "telegraf"."default".packets
''')
		.period(10s)
		.every(10s)
	|movingAverage('value', 2)
	|httpOut('TestBatch_MovingAverage')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "movingAverage"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						1002.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						1006.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						1008.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						1006.5,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_MovingAverage", script, 21*time.Second, er, false)
}
func TestBatch_CumulativeSum(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT "value"
		FROM "telegraf"."default".packets
''')
		.period(10s)
		.every(10s)
	|cumulativeSum('value')
	|httpOut('TestBatch_CumulativeSum')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "cumulativeSum"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 12, 0, time.UTC),
						10.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 14, 0, time.UTC),
						30.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 16, 0, time.UTC),
						60.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 18, 0, time.UTC),
						100.0,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_CumulativeSum", script, 31*time.Second, er, false)
}

func TestBatch_SimpleMR(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s), 'cpu')
	|count('mean')
	|window()
		.period(20s)
		.every(20s)
	|sum('count')
	|httpOut('TestBatch_SimpleMR')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu-total"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					10.0,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu0"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					10.0,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu1"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er, false)
}

func TestBatch_Where_NoSideEffect(t *testing.T) {

	var script = `
var data = batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s), 'cpu')
	|where(lambda: "mean" > 85)

// Unused where clause should not side-effect
data
	|where(lambda: FALSE)

data
	|httpOut('TestBatch_SimpleMR')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu-total"},
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 20, 0, time.UTC),
						91.06416290101595,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 22, 0, time.UTC),
						85.9694442394385,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 24, 0, time.UTC),
						90.62985736134186,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 26, 0, time.UTC),
						86.45443196005628,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
						88.97243107764031,
					},
				},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu0"},
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 20, 0, time.UTC),
						85.08910891088406,
					},
				},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu1"},
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 20, 0, time.UTC),
						96.49999999996908,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 22, 0, time.UTC),
						93.46464646468584,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 24, 0, time.UTC),
						95.00950095007724,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 26, 0, time.UTC),
						92.99999999998636,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
						90.99999999998545,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er, false)
}

func TestBatch_CountEmptyBatch(t *testing.T) {
	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA'
''')
		.period(10s)
		.every(10s)
		.groupBy('cpu')
	|where(lambda: "mean" < 10)
	|count('mean')
	|httpOut('TestBatch_CountEmptyBatch')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu-total"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					0.0,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu0"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					0.0,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu1"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					0.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_CountEmptyBatch", script, 30*time.Second, er, false)
}

func TestBatch_SumEmptyBatch(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA'
''')
		.period(10s)
		.every(10s)
		.groupBy('cpu')
	|where(lambda: "mean" < 10)
	|sum('mean')
	|httpOut('TestBatch_CountEmptyBatch')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu-total"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					0.0,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu0"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					0.0,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu1"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					0.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_CountEmptyBatch", script, 30*time.Second, er, false)
}

func TestBatch_GroupBy_TimeOffset(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s, 1s), 'cpu')
	|count('mean')
	|window()
		.period(20s)
		.every(20s)
	|sum('count')
	|httpOut('TestBatch_SimpleMR')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu-total"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					10.0,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu0"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					10.0,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu1"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er, false)
}

func TestBatch_Default(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA' AND "cpu" = 'cpu-total'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))
	|default()
		.field('mean', 90.0)
		.tag('dc', 'sfc')
	|groupBy('dc')
	|sum('mean')
	|httpOut('TestBatch_Default')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"dc": "sfc"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
					444.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Default", script, 30*time.Second, er, false)
}

func TestBatch_Delete(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu-total'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))
	|delete()
		.field('mean')
		.tag('dc')
	|default()
		.field('mean', 10.0)
		.tag('dc', 'sfc')
	|groupBy('dc')
	|sum('mean')
	|httpOut('TestBatch_Delete')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"dc": "sfc"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
					50.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Delete", script, 30*time.Second, er, false)
}
func TestBatch_Delete_GroupBy(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu-total'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s), 'dc')
	|delete()
		.field('mean')
		.tag('dc')
	|default()
		.field('mean', 10.0)
	|sum('mean')
	|httpOut('TestBatch_Delete_GroupBy')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Tags:    nil,
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 18, 0, time.UTC),
					50.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Delete_GroupBy", script, 30*time.Second, er, false)
}

func TestBatch_DoubleGroupBy(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA' AND "cpu" != 'cpu-total'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s), 'cpu')
	|groupBy()
	|max('mean')
	|httpOut('TestBatch_SimpleMR')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu1"},
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 18, 0, time.UTC),
					95.98484848485191,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er, false)
}

func TestBatch_GroupByMeasurement(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default"./cpu_.*/
		WHERE "host" = 'serverA'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s), 'cpu')
		.groupByMeasurement()
	|max('mean')
	|httpOut('TestBatch_GroupByMeasurement')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_user",
				Tags:    map[string]string{"cpu": "cpu-total"},
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					8.97243107764031,
				}},
			},
			{
				Name:    "cpu_usage_user",
				Tags:    map[string]string{"cpu": "cpu0"},
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					8.00000000002001,
				}},
			},
			{
				Name:    "cpu_usage_user",
				Tags:    map[string]string{"cpu": "cpu1"},
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					6.49999999996908,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu-total"},
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					91.06416290101595,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu0"},
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					85.08910891088406,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu1"},
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					96.49999999996908,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_GroupByMeasurement", script, 30*time.Second, er, true)
}
func TestBatch_GroupByNodeByMeasurement(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default"./cpu_.*/
		WHERE "host" = 'serverA'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s), 'cpu')
	|groupBy('cpu')
		.byMeasurement()
	|max('mean')
	|httpOut('TestBatch_GroupByMeasurement')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_user",
				Tags:    map[string]string{"cpu": "cpu-total"},
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 18, 0, time.UTC),
					9.90919811320221,
				}},
			},
			{
				Name:    "cpu_usage_user",
				Tags:    map[string]string{"cpu": "cpu0"},
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 18, 0, time.UTC),
					5.93434343435388,
				}},
			},
			{
				Name:    "cpu_usage_user",
				Tags:    map[string]string{"cpu": "cpu1"},
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 18, 0, time.UTC),
					6.54015887023496,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu-total"},
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 18, 0, time.UTC),
					91.01699558842134,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu0"},
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 18, 0, time.UTC),
					85.93434343435388,
				}},
			},
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu1"},
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 18, 0, time.UTC),
					95.98484848485191,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_GroupByMeasurement", script, 30*time.Second, er, true)
}

func TestBatch_AlertAll(t *testing.T) {
	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA' AND "cpu" != 'cpu-total'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s), 'cpu')
	|alert()
		.all()
		.crit(lambda:"mean" > 92)
	|httpOut('TestBatch_SimpleMR')
`

	// Expect no result since the condition is not met.
	er := kapacitor.Result{Series: imodels.Rows{}}

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er, false)

	script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA' AND "cpu" != 'cpu-total'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s), 'cpu')
	|alert()
		.all()
		.crit(lambda:"mean" > 90)
		.levelField('level')
	|httpOut('TestBatch_SimpleMR')
`

	er = kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu1"},
				Columns: []string{"time", "level", "mean"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 20, 0, time.UTC),
						"CRITICAL",
						96.49999999996908,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 22, 0, time.UTC),
						"CRITICAL",
						93.46464646468584,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 24, 0, time.UTC),
						"CRITICAL",
						95.00950095007724,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 26, 0, time.UTC),
						"CRITICAL",
						92.99999999998636,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
						"CRITICAL",
						90.99999999998545,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er, false)
}
func TestBatch_AlertLevelField(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA' AND "cpu" != 'cpu-total'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s), 'cpu')
	|alert()
		.crit(lambda:"mean" > 95)
		.levelField('level')
		.idField('id')
	|httpOut('TestBatch_SimpleMR')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu1"},
				Columns: []string{"time", "id", "level", "mean"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 20, 0, time.UTC),
						"cpu_usage_idle:cpu=cpu1",
						"CRITICAL",
						96.49999999996908,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 22, 0, time.UTC),
						"cpu_usage_idle:cpu=cpu1",
						"CRITICAL",
						93.46464646468584,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 24, 0, time.UTC),
						"cpu_usage_idle:cpu=cpu1",
						"CRITICAL",
						95.00950095007724,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 26, 0, time.UTC),
						"cpu_usage_idle:cpu=cpu1",
						"CRITICAL",
						92.99999999998636,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
						"cpu_usage_idle:cpu=cpu1",
						"CRITICAL",
						90.99999999998545,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er, false)
}

func TestBatch_AlertLevelTag(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA' AND "cpu" != 'cpu-total'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s), 'cpu')
	|alert()
		.crit(lambda:"mean" > 95)
		.levelTag('level')
		.idTag('id')
	|httpOut('TestBatch_SimpleMR')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu1", "level": "CRITICAL", "id": "cpu_usage_idle:cpu=cpu1"},
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 20, 0, time.UTC),
						96.49999999996908,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 22, 0, time.UTC),
						93.46464646468584,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 24, 0, time.UTC),
						95.00950095007724,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 26, 0, time.UTC),
						92.99999999998636,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
						90.99999999998545,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er, false)
}

func TestBatch_AlertDuration(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA' AND "cpu" != 'cpu-total'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s), 'cpu')
	|alert()
		.crit(lambda:"mean" > 95)
		.durationField('duration')
	|httpOut('TestBatch_SimpleMR')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu1"},
				Columns: []string{"time", "duration", "mean"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 20, 0, time.UTC),
						float64(14 * time.Second),
						96.49999999996908,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 22, 0, time.UTC),
						float64(14 * time.Second),
						93.46464646468584,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 24, 0, time.UTC),
						float64(14 * time.Second),
						95.00950095007724,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 26, 0, time.UTC),
						float64(14 * time.Second),
						92.99999999998636,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
						float64(14 * time.Second),
						90.99999999998545,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er, false)
}

func TestBatch_AlertMessage(t *testing.T) {

	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA' AND "cpu" != 'cpu-total'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s), 'cpu')
	|alert()
		.crit(lambda:"mean" > 95)
		.messageField('msg')
	|httpOut('TestBatch_SimpleMR')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Tags:    map[string]string{"cpu": "cpu1"},
				Columns: []string{"time", "mean", "msg"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 20, 0, time.UTC),
						96.49999999996908,
						"cpu_usage_idle:cpu=cpu1 is CRITICAL",
					},
					{
						time.Date(1971, 1, 1, 0, 0, 22, 0, time.UTC),
						93.46464646468584,
						"cpu_usage_idle:cpu=cpu1 is CRITICAL",
					},
					{
						time.Date(1971, 1, 1, 0, 0, 24, 0, time.UTC),
						95.00950095007724,
						"cpu_usage_idle:cpu=cpu1 is CRITICAL",
					},
					{
						time.Date(1971, 1, 1, 0, 0, 26, 0, time.UTC),
						92.99999999998636,
						"cpu_usage_idle:cpu=cpu1 is CRITICAL",
					},
					{
						time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
						90.99999999998545,
						"cpu_usage_idle:cpu=cpu1 is CRITICAL",
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er, false)
}

func TestBatch_AlertStateChangesOnly(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := kapacitor.AlertData{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&ad)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		if rc := atomic.LoadInt32(&requestCount); rc == 1 {
			expAd := kapacitor.AlertData{
				ID:      "cpu_usage_idle:cpu=cpu-total",
				Message: "cpu_usage_idle:cpu=cpu-total is CRITICAL",
				Time:    time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
				Level:   alert.Critical,
			}
			ad.Data = influxql.Result{}
			if eq, msg := compareAlertData(expAd, ad); !eq {
				t.Error(msg)
			}
		} else {
			expAd := kapacitor.AlertData{
				ID:       "cpu_usage_idle:cpu=cpu-total",
				Message:  "cpu_usage_idle:cpu=cpu-total is OK",
				Time:     time.Date(1971, 1, 1, 0, 0, 38, 0, time.UTC),
				Duration: 38 * time.Second,
				Level:    alert.OK,
			}
			ad.Data = influxql.Result{}
			if eq, msg := compareAlertData(expAd, ad); !eq {
				t.Errorf("unexpected alert data for request: %d %s", rc, msg)
			}
		}
	}))
	defer ts.Close()
	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA' AND "cpu" != 'cpu-total'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s), 'cpu')
	|alert()
		.crit(lambda:"mean" > 90)
		.stateChangesOnly()
		.levelField('level')
		.details('')
		.post('` + ts.URL + `')
`
	clock, et, replayErr, tm := testBatcher(t, "TestBatch_AlertStateChangesOnly", script)
	defer tm.Close()

	err := fastForwardTask(clock, et, replayErr, tm, 40*time.Second)
	if err != nil {
		t.Error(err)
	}
	if exp, rc := 2, int(atomic.LoadInt32(&requestCount)); rc != exp {
		t.Errorf("got %v exp %v", rc, exp)
	}
}

func TestBatch_AlertStateChangesOnlyExpired(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := kapacitor.AlertData{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&ad)
		if err != nil {
			t.Fatal(err)
		}
		// We don't care about the data for this test
		ad.Data = influxql.Result{}
		var expAd kapacitor.AlertData
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)
		if rc < 3 {
			expAd = kapacitor.AlertData{
				ID:       "cpu_usage_idle:cpu=cpu-total",
				Message:  "cpu_usage_idle:cpu=cpu-total is CRITICAL",
				Time:     time.Date(1971, 1, 1, 0, 0, int(rc-1)*20, 0, time.UTC),
				Duration: time.Duration(rc-1) * 20 * time.Second,
				Level:    alert.Critical,
			}
		} else {
			expAd = kapacitor.AlertData{
				ID:       "cpu_usage_idle:cpu=cpu-total",
				Message:  "cpu_usage_idle:cpu=cpu-total is OK",
				Time:     time.Date(1971, 1, 1, 0, 0, 38, 0, time.UTC),
				Duration: 38 * time.Second,
				Level:    alert.OK,
			}
		}
		if eq, msg := compareAlertData(expAd, ad); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()
	var script = `
batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "host" = 'serverA' AND "cpu" != 'cpu-total'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s), 'cpu')
	|alert()
		.crit(lambda:"mean" > 90)
		.stateChangesOnly(15s)
		.levelField('level')
		.details('')
		.post('` + ts.URL + `')
`
	clock, et, replayErr, tm := testBatcher(t, "TestBatch_AlertStateChangesOnly", script)
	defer tm.Close()

	err := fastForwardTask(clock, et, replayErr, tm, 40*time.Second)
	if err != nil {
		t.Error(err)
	}
	if exp, rc := 3, int(atomic.LoadInt32(&requestCount)); rc != exp {
		t.Errorf("got %v exp %v", rc, exp)
	}
}

func TestBatch_Flatten(t *testing.T) {
	var script = `
batch
	|query('SELECT value FROM "telegraf"."default"."request_latency"')
		.period(10s)
		.every(10s)
		.groupBy('dc','service')
	|groupBy('dc')
	|flatten()
		.on('service')
		.tolerance(5s)
    |httpOut('TestBatch_Flatten')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A"},
				Columns: []string{"time", "auth.value", "cart.value", "log.value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
						4.0,
						8.0,
						7.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 15, 0, time.UTC),
						2.0,
						3.0,
						1.0,
					},
				},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B"},
				Columns: []string{"time", "auth.value", "cart.value", "log.value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
						9.0,
						3.0,
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 15, 0, time.UTC),
						6.0,
						7.0,
						4.0,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Flatten", script, 40*time.Second, er, true)
}

func TestBatch_Combine_All(t *testing.T) {
	var script = `
batch
	|query('SELECT value FROM "telegraf"."default"."request_latency"')
		.period(10s)
		.every(10s)
		.groupBy('dc','service')
	|groupBy('dc')
	|combine(lambda: TRUE, lambda: TRUE)
		.as('first', 'second')
		.tolerance(5s)
		.delimiter('.')
	|groupBy('first.service', 'second.service', 'dc')
	|eval(lambda: "first.value" / "second.value")
		.as('ratio')
    |httpOut('TestBatch_Combine')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "first.service": "cart", "second.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 15, 0, time.UTC),
					3.0 / 2.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "first.service": "cart", "second.service": "log"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 15, 0, time.UTC),
					3.0 / 1.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "first.service": "auth", "second.service": "log"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 15, 0, time.UTC),
					2.0 / 1.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "first.service": "cart", "second.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 15, 0, time.UTC),
					7.0 / 6.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "first.service": "cart", "second.service": "log"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 15, 0, time.UTC),
					7.0 / 4.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "first.service": "auth", "second.service": "log"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 15, 0, time.UTC),
					6.0 / 4.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Combine", script, 40*time.Second, er, true)
}

func TestBatch_Combine_Filtered(t *testing.T) {
	var script = `
batch
	|query('SELECT value FROM "telegraf"."default"."request_latency"')
		.period(10s)
		.every(10s)
		.groupBy('dc','service')
	|groupBy('dc')
	|combine(lambda: "service" == 'auth', lambda: TRUE)
		.as('auth', 'other')
		.tolerance(5s)
		.delimiter('.')
	|groupBy('auth.service', 'other.service', 'dc')
	|eval(lambda: "auth.value" / "other.value")
		.as('ratio')
    |httpOut('TestBatch_Combine')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "other.service": "log", "auth.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 15, 0, time.UTC),
					2.0 / 1.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "other.service": "cart", "auth.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 15, 0, time.UTC),
					2.0 / 3.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "other.service": "log", "auth.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 15, 0, time.UTC),
					6.0 / 4.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "other.service": "cart", "auth.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 15, 0, time.UTC),
					6.0 / 7.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Combine", script, 30*time.Second, er, true)
}

func TestBatch_Combine_All_Triples(t *testing.T) {
	var script = `
batch
	|query('SELECT value FROM "telegraf"."default"."request_latency"')
		.period(10s)
		.every(10s)
		.groupBy('dc','service')
	|groupBy('dc')
	|combine(lambda: TRUE, lambda: TRUE, lambda: TRUE)
		.as('first', 'second','third')
		.tolerance(5s)
		.delimiter('.')
	|groupBy('first.service', 'second.service', 'third.service', 'dc')
	|eval(lambda: "first.value" + "second.value" + "third.value")
		.as('sum')
    |httpOut('TestBatch_Combine')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "first.service": "cart", "second.service": "auth", "third.service": "log"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 15, 0, time.UTC),
					6.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "first.service": "cart", "second.service": "auth", "third.service": "log"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 15, 0, time.UTC),
					17.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Combine", script, 30*time.Second, er, true)
}

func TestBatch_Join(t *testing.T) {

	var script = `
var cpu0 = batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu0'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))

var cpu1 = batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu1'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))

cpu0
	|join(cpu1)
		.as('cpu0', 'cpu1')
	|count('cpu0.mean')
	|window()
		.period(20s)
		.every(20s)
	|sum('count')
	|httpOut('TestBatch_Join')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Join", script, 30*time.Second, er, false)
}
func TestBatch_Join_Delimiter(t *testing.T) {

	var script = `
var cpu0 = batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu0'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))

var cpu1 = batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu1'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))

cpu0
	|join(cpu1)
		.as('cpu0', 'cpu1')
		.delimiter('~')
	|count('cpu0~mean')
	|window()
		.period(20s)
		.every(20s)
	|sum('count')
	|httpOut('TestBatch_Join')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Join", script, 30*time.Second, er, false)
}
func TestBatch_Join_DelimiterEmpty(t *testing.T) {

	var script = `
var cpu0 = batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu0'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))

var cpu1 = batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu1'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))

cpu0
	|join(cpu1)
		.as('cpu0', 'cpu1')
		.delimiter('')
	|count('cpu0mean')
	|window()
		.period(20s)
		.every(20s)
	|sum('count')
	|httpOut('TestBatch_Join')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Join", script, 30*time.Second, er, false)
}

func TestBatch_JoinTolerance(t *testing.T) {

	var script = `
var cpu0 = batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu0'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))

var cpu1 = batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu1'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))

cpu0
	|join(cpu1)
		.as('cpu0', 'cpu1')
		.tolerance(1s)
	|count('cpu0.mean')
	|window()
		.period(20s)
		.every(20s)
	|sum('count')
	|httpOut('TestBatch_JoinTolerance')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_JoinTolerance", script, 30*time.Second, er, false)
}

func TestBatch_Join_NoFill(t *testing.T) {

	var script = `
var cpu0 = batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu0'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))

var cpu1 = batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu1'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))

cpu0
	|join(cpu1)
		.as('cpu0', 'cpu1')
	|eval(lambda: "cpu0.mean" + "cpu1.mean")
		.as('cpu')
	|sum('cpu')
	|window()
		.period(20s)
		.every(20s)
	|sum('sum')
	|httpOut('TestBatch_Join_Fill')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					876.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Join_Fill", script, 30*time.Second, er, false)
}

func TestBatch_Join_Fill_Num(t *testing.T) {

	var script = `
var cpu0 = batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu0'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))

var cpu1 = batch
	|query('''
		SELECT mean("value")
		FROM "telegraf"."default".cpu_usage_idle
		WHERE "cpu" = 'cpu1'
''')
		.period(10s)
		.every(10s)
		.groupBy(time(2s))

cpu0
	|join(cpu1)
		.as('cpu0', 'cpu1')
		.fill(100.0)
	|eval(lambda: "cpu0.mean" + "cpu1.mean")
		.as('cpu')
	|sum('cpu')
	|window()
		.period(20s)
		.every(20s)
	|sum('sum')
	|httpOut('TestBatch_Join_Fill')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "cpu_usage_idle",
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
					1178.0,
				}},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Join_Fill", script, 30*time.Second, er, false)
}

func TestBatch_JoinOn(t *testing.T) {

	var script = `
var errorsByServiceGlobal = batch
	|query('''
		SELECT sum("value")
		FROM "telegraf"."default".errors
''')
		.period(10s)
		.every(10s)
		.groupBy(time(5s),'service')

var errorsByServiceDC = batch
	|query('''
		SELECT first("value") as value
		FROM "telegraf"."default".errors
''')
		.period(10s)
		.every(10s)
		.groupBy(time(5s),'dc', 'service')

errorsByServiceGlobal
	|join(errorsByServiceDC)
		.as('service', 'dc')
		.on('service')
		.streamName('dc_error_percent')
	|eval(lambda: "dc.value" / "service.sum")
		.keep()
		.as('value')
	|httpOut('TestBatch_JoinOn')
`

	er := kapacitor.Result{
		Series: imodels.Rows{
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "slc", "service": "cart"},
				Columns: []string{"time", "dc.value", "service.sum", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						8.0,
						11.0,
						8.0 / 11.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						3.0,
						10.0,
						3.0 / 10.0,
					},
				},
			},
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "nyc", "service": "cart"},
				Columns: []string{"time", "dc.value", "service.sum", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
						11.0,
						3.0 / 11.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						7.0,
						10.0,
						7.0 / 10.0,
					},
				},
			},
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "slc", "service": "login"},
				Columns: []string{"time", "dc.value", "service.sum", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
						13.0,
						4.0 / 13.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						2.0,
						8.0,
						2.0 / 8.0,
					},
				},
			},
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "nyc", "service": "login"},
				Columns: []string{"time", "dc.value", "service.sum", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						9.0,
						13.0,
						9.0 / 13.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						6.0,
						8.0,
						6.0 / 8.0,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_JoinOn", script, 30*time.Second, er, true)
}

func TestBatch_JoinOn_Fill_Num(t *testing.T) {

	var script = `
var maintlock = batch
    |query('SELECT count FROM "db"."rp"."maintlock"')
        .period(10s)
        .every(10s)
        .groupBy('host')
        .align()

batch
    |query('SELECT count FROM "telegraf"."default"."disk"')
        .period(10s)
        .every(10s)
        .groupBy('host', 'path')
        .align()
    |join(maintlock)
        .as('disk', 'maintlock')
        .on('host')
        .fill(0.0)
        .tolerance(1s)
    |default()
        .field('maintlock.count', 0)
    |httpOut('TestBatch_JoinOn_Fill')
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

	testBatcherWithOutput(t, "TestBatch_JoinOn_Fill", script, 30*time.Second, er, true)
}

func TestBatch_JoinOn_Fill_Null(t *testing.T) {

	var script = `
var maintlock = batch
    |query('SELECT count FROM "db"."rp"."maintlock"')
        .period(10s)
        .every(10s)
        .groupBy('host')
        .align()

batch
    |query('SELECT count FROM "telegraf"."default"."disk"')
        .period(10s)
        .every(10s)
        .groupBy('host', 'path')
        .align()
    |join(maintlock)
        .as('disk', 'maintlock')
        .on('host')
        .fill('null')
        .tolerance(1s)
    |default()
        .field('maintlock.count', 0)
    |httpOut('TestBatch_JoinOn_Fill')
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

	testBatcherWithOutput(t, "TestBatch_JoinOn_Fill", script, 30*time.Second, er, true)
}

// Helper test function for batcher
func testBatcher(t *testing.T, name, script string) (clock.Setter, *kapacitor.ExecutingTask, <-chan error, *kapacitor.TaskMaster) {
	if testing.Verbose() {
		wlog.SetLevel(wlog.DEBUG)
	} else {
		wlog.SetLevel(wlog.OFF)
	}

	// Create a new execution env
	tm := kapacitor.NewTaskMaster("testBatcher", logService)
	tm.HTTPDService = httpService
	tm.TaskStore = taskStore{}
	tm.DeadmanService = deadman{}
	tm.Open()

	// Create task
	task, err := tm.NewTask(name, script, kapacitor.BatchTask, dbrps, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Load test data
	var allData []io.ReadCloser
	var data io.ReadCloser
	for i := 0; err == nil; {
		f := fmt.Sprintf("%s.%d.brpl", name, i)
		data, err = os.Open(path.Join("data", f))
		if err == nil {
			allData = append(allData, data)
			i++
		}
	}
	if len(allData) == 0 {
		t.Fatal("could not find any data files for", name)
	}

	//Start the task
	et, err := tm.StartTask(task)
	if err != nil {
		t.Fatal(err)
	}

	// Replay test data to executor
	batches := tm.BatchCollectors(name)
	// Use 1971 so that we don't get true negatives on Epoch 0 collisions
	c := clock.New(time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC))
	replayErr := kapacitor.ReplayBatchFromIO(c, allData, batches, false)

	t.Log(string(et.Task.Dot()))
	return c, et, replayErr, tm
}

func testBatcherWithOutput(
	t *testing.T,
	name,
	script string,
	duration time.Duration,
	er kapacitor.Result,
	ignoreOrder bool,
) {
	clock, et, replayErr, tm := testBatcher(t, name, script)
	defer tm.Close()

	err := fastForwardTask(clock, et, replayErr, tm, duration)
	if err != nil {
		t.Error(err)
	}

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
