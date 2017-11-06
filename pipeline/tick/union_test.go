package tick_test

import (
	"testing"

	"github.com/influxdata/kapacitor/pipeline"
)

func TestUnion(t *testing.T) {
	type args struct {
		nodes  []pipeline.Node
		rename string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "union of a stream and batch",
			args: args{
				nodes: []pipeline.Node{
					&pipeline.StreamNode{},
					&pipeline.BatchNode{},
					&pipeline.StreamNode{},
				},
			},
			want: `var from4 = stream
    |from()

var union6 = batch
    |query('select cpu_usage from cpu')
    |union(from4)

stream
    |from()
    |log()
        .level('INFO')
    |union(union6)
`,
		},
		{
			name: "union of a stream and batch with rename",
			args: args{
				nodes: []pipeline.Node{
					&pipeline.StreamNode{},
					&pipeline.BatchNode{},
					&pipeline.StreamNode{},
				},
				rename: "renamed",
			},
			want: `var from4 = stream
    |from()

var union6 = batch
    |query('select cpu_usage from cpu')
    |union(from4)
        .rename('renamed')

stream
    |from()
    |log()
        .level('INFO')
    |union(union6)
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipe := pipeline.CreatePipelineSources(tt.args.nodes...)
			stream := tt.args.nodes[0].(*pipeline.StreamNode)
			batch := tt.args.nodes[1].(*pipeline.BatchNode)
			stream2 := tt.args.nodes[2].(*pipeline.StreamNode)
			query := batch.Query("select cpu_usage from cpu")
			union := stream.From().Union(query)
			union.Rename = tt.args.rename
			logger := stream2.From().Log()
			union.Union(logger)

			got, err := PipelineTick(pipe)
			if err != nil {
				t.Fatalf("Unexpected error building pipeline %v", err)
			}
			if got != tt.want {
				t.Errorf("%q. TestUnion = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
