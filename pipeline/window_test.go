package pipeline

import (
	"bytes"
	"testing"
	"time"
)

func TestWindowNode_Tick(t *testing.T) {
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
			want: `|window().period(1s).every(1h).align().fillPeriod()`,
		},
		{
			name: "window with period count and every count",
			args: args{
				periodCount: 10,
				everyCount:  15,
				fillPeriod:  true,
			},
			want: `|window().periodCount(10).everyCount(15).fillPeriod()`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newWindowNode()
			n.Period = tt.args.period
			n.Every = tt.args.every
			n.AlignFlag = tt.args.align
			n.FillPeriodFlag = tt.args.fillPeriod
			n.PeriodCount = tt.args.periodCount
			n.EveryCount = tt.args.everyCount

			var buf bytes.Buffer
			n.Tick(&buf)
			got := buf.String()
			if got != tt.want {
				t.Errorf("%q. TestWindowNode_Tick() =\n%v\n want\n%v\n", tt.name, got, tt.want)
			}
		})
	}
}
