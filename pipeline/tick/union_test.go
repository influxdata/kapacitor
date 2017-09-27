package tick_test

import (
	"testing"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/pipeline/tick"
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
			name: "No Nodes",
			want: `|union()`,
		},
		{
			name: "union of a stream and batch",
			args: args{
				nodes: []pipeline.Node{
					&pipeline.StreamNode{},
					&pipeline.StreamNode{},
				},
			},
			want: `|union(stream, batch)`,
		},
		{
			name: "union of a stream and batch with rename",
			args: args{
				nodes: []pipeline.Node{

					&pipeline.StreamNode{},
					&pipeline.BatchNode{},
				},
				rename: "renamed",
			},
			want: `|union(stream, batch).rename('renamed')`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = pipeline.CreatePipelineSources(tt.args.nodes...)
			var u pipeline.UnionNode
			if len(tt.args.nodes) > 0 {
				stream := tt.args.nodes[0].(*pipeline.StreamNode)
				u = *stream.From().Union(tt.args.nodes[1])
			}

			u.Rename = tt.args.rename

			ast := tick.AST{
				Node: &NullNode{},
			}

			ast.Union(&u)
			got := ast.TICKScript()
			if got != tt.want {
				t.Errorf("%q. TestUnion = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
