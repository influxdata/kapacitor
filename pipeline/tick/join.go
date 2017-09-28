package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Join converts the join pipeline node into the TICKScript AST
type Join struct {
	Function
}

// NewJoin creates a Join function builder
func NewJoin(parents []ast.Node) *Join {
	return &Join{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a join ast.Node
func (n *Join) Build(j *pipeline.JoinNode) (ast.Node, error) {
	joined := []interface{}{}
	for _, p := range n.Parents[1:] {
		joined = append(joined, p)
	}
	n.Pipe("join", joined...).
		Dot("as", args(j.Names)).
		Dot("on", args(j.Dimensions)).
		Dot("delimiter", j.Delimiter).
		Dot("streamName", j.StreamName).
		Dot("tolerance", j.Tolerance).
		DotNotNil("fill", j.Fill)
	return n.prev, n.err
}
