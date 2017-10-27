package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// JoinNode converts the JoinNode pipeline node into the TICKScript AST
type JoinNode struct {
	Function
}

// NewJoin creates a JoinNode function builder
func NewJoin(parents []ast.Node) *JoinNode {
	return &JoinNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a JoinNode ast.Node
func (n *JoinNode) Build(j *pipeline.JoinNode) (ast.Node, error) {
	joined := []interface{}{}
	for _, p := range n.Parents[1:] {
		joined = append(joined, p)
	}
	n.Pipe("join", joined...).
		Dot("as", args(j.Names)...).
		Dot("on", args(j.Dimensions)...).
		Dot("delimiter", j.Delimiter).
		Dot("streamName", j.StreamName).
		Dot("tolerance", j.Tolerance).
		DotNotNil("fill", j.Fill)
	return n.prev, n.err
}
