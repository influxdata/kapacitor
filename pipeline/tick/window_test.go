package tick_test

import (
	"testing"
	"time"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/pipeline/tick"
)

func TestWindow(t *testing.T) {
	type args struct {
		period      time.Duration
		every       time.Duration
		align       bool
		fillPeriod  bool
		periodCount int64
		everyCount  int64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "window with period and every",
			args: args{
				period:     time.Second,
				every:      time.Hour,
				align:      true,
				fillPeriod: true,
			},
			want: "\n    |window()\n        .period(1s)\n        .every(1h)\n        .align()\n        .fillPeriod()",
		},
		{
			name: "window with period count and every count",
			args: args{
				periodCount: 10,
				everyCount:  15,
				fillPeriod:  true,
			},
			want: "\n    |window()\n        .periodCount(10)\n        .everyCount(15)\n        .fillPeriod()",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &pipeline.WindowNode{}
			w.Period = tt.args.period
			w.Every = tt.args.every
			w.AlignFlag = tt.args.align
			w.FillPeriodFlag = tt.args.fillPeriod
			w.PeriodCount = tt.args.periodCount
			w.EveryCount = tt.args.everyCount

			ast := tick.AST{
				Node: &NullNode{},
			}

			ast.Window(w)
			got := ast.TICKScript()
			if got != tt.want {
				t.Errorf("%q. TestWindow() =\n%v\n want\n%v\n", tt.name, got, tt.want)
			}
		})
	}
}
