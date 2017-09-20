package pipeline

import (
	"bytes"
	"testing"
	"time"
)

func TestStatsNode_Tick(t *testing.T) {
	type args struct {
		interval time.Duration
		align    bool
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "stats with alignment",
			args: args{
				interval: time.Hour,
				align:    true,
			},
			want: `|stats(1h).align()`,
		},
		{
			name: "stats without alignment",
			args: args{
				interval: 24 * time.Hour,
			},
			want: `|stats(1d)`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newStatsNode(nil, tt.args.interval)
			n.AlignFlag = tt.args.align

			var buf bytes.Buffer
			n.Tick(&buf)
			got := buf.String()
			if got != tt.want {
				t.Errorf("%q. TestStatsNode_Tick() =\n%v\n want\n%v\n", tt.name, got, tt.want)
			}
		})
	}
}
