package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// DeleteNode converts the Delete pipeline node into the TICKScript AST
type DeleteNode struct {
	Function
}

// NewDelete creates a Delete function builder
func NewDelete(parents []ast.Node) *DeleteNode {
	return &DeleteNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a Delete ast.Node
func (n *DeleteNode) Build(d *pipeline.DeleteNode) (ast.Node, error) {
	n.Pipe("delete")
	for _, f := range d.Fields {
		n.Dot("field", f)
	}
	for _, t := range d.Tags {
		n.Dot("tag", t)
	}
	return n.prev, n.err
}
