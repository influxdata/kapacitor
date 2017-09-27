package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Delete converts the Delete pipeline node into the TICKScript AST
type Delete struct {
	Function
}

// NewDelete creates a Delete function builder
func NewDelete(parents []ast.Node) *Delete {
	return &Delete{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a Delete ast.Node
func (n *Delete) Build(d *pipeline.DeleteNode) (ast.Node, error) {
	n.Pipe("delete")
	for k, v := range d.Fields {
		n.Dot("field", k, v)
	}
	for k, v := range d.Tags {
		n.Dot("tag", k, v)
	}
	return n.prev, n.err
}
