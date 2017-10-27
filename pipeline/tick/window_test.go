package tick_test

import (
	"testing"
	"time"

	"github.com/influxdata/kapacitor/pipeline"
)

func TestWindowNode(t *testing.T) {
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
			want: `stream
    |from()
    |window()
        .period(1s)
        .every(1h)
        .align()
        .fillPeriod()
`,
		},
		{
			name: "window with period count and every count",
			args: args{
				periodCount: 10,
				everyCount:  15,
				fillPeriod:  false,
			},
			want: `stream
    |from()
    |window()
        .periodCount(10)
        .everyCount(15)
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := &pipeline.StreamNode{}
			pipe := pipeline.CreatePipelineSources(stream)

			w := stream.From().Window()
			w.Period = tt.args.period
			w.Every = tt.args.every
			w.AlignFlag = tt.args.align
			w.FillPeriodFlag = tt.args.fillPeriod
			w.PeriodCount = tt.args.periodCount
			w.EveryCount = tt.args.everyCount

			got, err := PipelineTick(pipe)
			if err != nil {
				t.Fatalf("Unexpected error building pipeline %v", err)
			}
			if got != tt.want {
				t.Errorf("%q. TestWindow() =\n%v\n want\n%v\n", tt.name, got, tt.want)
			}
		})
	}
}
