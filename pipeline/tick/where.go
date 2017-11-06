package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// WhereNode converts the where pipeline node into the TICKScript AST
type WhereNode struct {
	Function
}

// NewWhere creates a Where function builder
func NewWhere(parents []ast.Node) *WhereNode {
	return &WhereNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a where ast.Node
func (n *WhereNode) Build(w *pipeline.WhereNode) (ast.Node, error) {
	n.Pipe("where", w.Lambda)
	return n.prev, n.err
}
