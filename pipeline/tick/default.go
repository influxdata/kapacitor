package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Default converts the Default pipeline node into the TICKScript AST
type Default struct {
	Function
}

// NewDefault creates a Default function builder
func NewDefault(parents []ast.Node) *Default {
	return &Default{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a Default ast.Node
func (n *Default) Build(d *pipeline.DefaultNode) (ast.Node, error) {
	n.Pipe("default")
	for k, v := range d.Fields {
		n.Dot("field", k, v)
	}
	for k, v := range d.Tags {
		n.Dot("tag", k, v)
	}
	return n.prev, n.err
}
