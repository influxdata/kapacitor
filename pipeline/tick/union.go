package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Union converts the union pipeline node into the TICKScript AST
type Union struct {
	Function
}

// Build creates a union ast.Node
func (n *Union) Build(u *pipeline.UnionNode) (ast.Node, error) {
	n.Pipe("union", parents[1:]).
		Dot("rename", u.Rename)
	return n.prev, n.err
}
