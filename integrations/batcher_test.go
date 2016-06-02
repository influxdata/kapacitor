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
	"github.com/influxdata/wlog"
)

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

	testBatcherWithOutput(t, "TestBatch_Derivative", script, 21*time.Second, er)
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

	testBatcherWithOutput(t, "TestBatch_Derivative", script, 21*time.Second, er)
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

	testBatcherWithOutput(t, "TestBatch_DerivativeNN", script, 21*time.Second, er)
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

	testBatcherWithOutput(t, "TestBatch_DerivativeNN", script, 21*time.Second, er)
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
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						2000.0,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_Elapsed", script, 21*time.Second, er)
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

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er)
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
	|log().prefix('QUERY')
	|where(lambda: "mean" < 10)
	|log().prefix('WHERE')
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

	testBatcherWithOutput(t, "TestBatch_CountEmptyBatch", script, 30*time.Second, er)
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
	|log().prefix('QUERY')
	|where(lambda: "mean" < 10)
	|log().prefix('WHERE')
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

	testBatcherWithOutput(t, "TestBatch_CountEmptyBatch", script, 30*time.Second, er)
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

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er)
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

	testBatcherWithOutput(t, "TestBatch_Default", script, 30*time.Second, er)
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

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er)
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

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er)

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

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er)
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
						"cpu_usage_idle:cpu=cpu1,",
						"CRITICAL",
						96.49999999996908,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 22, 0, time.UTC),
						"cpu_usage_idle:cpu=cpu1,",
						"CRITICAL",
						93.46464646468584,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 24, 0, time.UTC),
						"cpu_usage_idle:cpu=cpu1,",
						"CRITICAL",
						95.00950095007724,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 26, 0, time.UTC),
						"cpu_usage_idle:cpu=cpu1,",
						"CRITICAL",
						92.99999999998636,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 28, 0, time.UTC),
						"cpu_usage_idle:cpu=cpu1,",
						"CRITICAL",
						90.99999999998545,
					},
				},
			},
		},
	}

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er)
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
				Tags:    map[string]string{"cpu": "cpu1", "level": "CRITICAL", "id": "cpu_usage_idle:cpu=cpu1,"},
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

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er)
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

	testBatcherWithOutput(t, "TestBatch_SimpleMR", script, 30*time.Second, er)
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
				ID:      "cpu_usage_idle:cpu=cpu-total,",
				Message: "cpu_usage_idle:cpu=cpu-total, is CRITICAL",
				Time:    time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
				Level:   kapacitor.CritAlert,
			}
			ad.Data = influxql.Result{}
			if eq, msg := compareAlertData(expAd, ad); !eq {
				t.Error(msg)
			}
		} else {
			expAd := kapacitor.AlertData{
				ID:       "cpu_usage_idle:cpu=cpu-total,",
				Message:  "cpu_usage_idle:cpu=cpu-total, is OK",
				Time:     time.Date(1971, 1, 1, 0, 0, 38, 0, time.UTC),
				Duration: 38 * time.Second,
				Level:    kapacitor.OKAlert,
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
				ID:       "cpu_usage_idle:cpu=cpu-total,",
				Message:  "cpu_usage_idle:cpu=cpu-total, is CRITICAL",
				Time:     time.Date(1971, 1, 1, 0, 0, int(rc-1)*20, 0, time.UTC),
				Duration: time.Duration(rc-1) * 20 * time.Second,
				Level:    kapacitor.CritAlert,
			}
		} else {
			expAd = kapacitor.AlertData{
				ID:       "cpu_usage_idle:cpu=cpu-total,",
				Message:  "cpu_usage_idle:cpu=cpu-total, is OK",
				Time:     time.Date(1971, 1, 1, 0, 0, 38, 0, time.UTC),
				Duration: 38 * time.Second,
				Level:    kapacitor.OKAlert,
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

	testBatcherWithOutput(t, "TestBatch_Join", script, 30*time.Second, er)
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

	testBatcherWithOutput(t, "TestBatch_JoinTolerance", script, 30*time.Second, er)
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
	ignoreOrder ...bool,
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
	if eq, msg := compareResults(er, result); !eq {
		t.Error(msg)
	}
}
