package pipeline

import (
	"bytes"
	"testing"
)

func TestUnionNode_Tick(t *testing.T) {
	type args struct {
		nodes  []Node
		typeOf EdgeType
		rename string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "No Nodes",
			args: args{
				typeOf: BatchEdge,
			},
			want: `|union()`,
		},
		{
			name: "union of a stream and batch",
			args: args{
				typeOf: BatchEdge,
				nodes: []Node{
					newStreamNode(),
					newBatchNode(),
				},
			},
			want: `|union(stream, batch)`,
		},
		{
			name: "union of a stream and batch with rename",
			args: args{
				typeOf: BatchEdge,
				nodes: []Node{
					newStreamNode(),
					newBatchNode(),
				},
				rename: "renamed",
			},
			want: `|union(stream, batch).rename('renamed')`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := &UnionNode{
				chainnode: newBasicChainNode("union", tt.args.typeOf, tt.args.typeOf),
				Rename:    tt.args.rename,
			}

			// Needed to create special unionNode construction logic to make sure parents were correct.
			pipeline := &Pipeline{}
			u.setPipeline(pipeline)
			for _, n := range tt.args.nodes {
				n.setPipeline(u.p)
				_ = u.p.assignID(n)
				u.children = append(u.children, n)
				n.addParent(u)
			}

			var buf bytes.Buffer
			u.Tick(&buf)
			got := buf.String()
			if got != tt.want {
				t.Errorf("%q. TestUnionNode_Tick() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
