package influxdb_test

import (
	"testing"
	"time"

	"github.com/influxdata/kapacitor/influxdb"
)

func TestPoint_Bytes(t *testing.T) {
	fields := map[string]interface{}{"value": float64(1), "another": int64(42)}
	tags := map[string]string{"host": "serverA", "dc": "nyc"}
	tm, _ := time.Parse(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z")
	tests := []struct {
		name      string
		precision string
		exp       string
		t         time.Time
	}{
		{
			name:      "zero time",
			precision: "",
			exp:       "cpu,dc=nyc,host=serverA another=42i,value=1",
		},
		{
			name:      "no precision",
			precision: "",
			exp:       "cpu,dc=nyc,host=serverA another=42i,value=1 946730096789012345",
			t:         tm,
		},
		{
			name:      "nanosecond precision",
			precision: "ns",
			exp:       "cpu,dc=nyc,host=serverA another=42i,value=1 946730096789012345",
			t:         tm,
		},
		{
			name:      "microsecond precision",
			precision: "u",
			exp:       "cpu,dc=nyc,host=serverA another=42i,value=1 946730096789012",
			t:         tm,
		},
		{
			name:      "millisecond precision",
			precision: "ms",
			exp:       "cpu,dc=nyc,host=serverA another=42i,value=1 946730096789",
			t:         tm,
		},
		{
			name:      "second precision",
			precision: "s",
			exp:       "cpu,dc=nyc,host=serverA another=42i,value=1 946730096",
			t:         tm,
		},
		{
			name:      "minute precision",
			precision: "m",
			exp:       "cpu,dc=nyc,host=serverA another=42i,value=1 15778834",
			t:         tm,
		},
		{
			name:      "hour precision",
			precision: "h",
			exp:       "cpu,dc=nyc,host=serverA another=42i,value=1 262980",
			t:         tm,
		},
	}

	for _, test := range tests {
		p := influxdb.Point{
			Name:   "cpu",
			Tags:   tags,
			Fields: fields,
			Time:   test.t,
		}

		if got, exp := string(p.Bytes(test.precision)), test.exp; got != exp {
			t.Errorf("%s: Bytes() mismatch:\n actual:	%v\n exp:		%v",
				test.name, got, exp)
		}
	}
}
