package tick_test

import (
	"testing"
	"time"

	"github.com/influxdata/kapacitor/pipeline"
)

func TestBarrierNode(t *testing.T) {
	type args struct {
		idle   time.Duration
		period time.Duration
		del    bool
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "barrier with idle",
			args: args{
				idle: time.Second,
			},
			want: `stream
    |from()
    |barrier()
        .idle(1s)
`,
		},
		{
			name: "barrier with period",
			args: args{
				period: time.Second,
			},
			want: `stream
    |from()
    |barrier()
        .period(1s)
`,
		},
		{
			name: "barrier with delete",
			args: args{
				del: true,
			},
			want: `stream
    |from()
    |barrier()
        .delete(TRUE)
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := &pipeline.StreamNode{}
			pipe := pipeline.CreatePipelineSources(stream)

			b := stream.From().Barrier()
			b.Idle = tt.args.idle
			b.Period = tt.args.period
			b.Delete = tt.args.del

			got, err := PipelineTick(pipe)
			if err != nil {
				t.Fatalf("Unexpected error building pipeline %v", err)
			}
			if got != tt.want {
				t.Errorf("%q. TestBarrier() =\n%v\n want\n%v\n", tt.name, got, tt.want)
			}
		})
	}
}
