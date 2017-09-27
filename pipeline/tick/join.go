package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Join converts the join pipeline node into the TICKScript AST
type Join struct {
	Function
}

// Build creates a join ast.Node
func (n *Join) Build(j *pipeline.JoinNode) (ast.Node, error) {
	n.Pipe("join", parents[1:]).
		Dot("rename", j.Rename).
		Dot("as", j.Names...).
		Dot("on", j.Dimensions...).
		Dot("streamName", j.StreamName).
		Dot("tolerance", j.Tolerance).
		Dot("fill", j.Fill)
	return n.prev, n.err
}
