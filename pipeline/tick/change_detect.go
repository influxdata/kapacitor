package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// ChangeDetectNode converts the ChangeDetect pipeline node into the TICKScript AST
type ChangeDetectNode struct {
	Function
}

// NewChangeDetect creates a ChangeDetect function builder
func NewChangeDetect(parents []ast.Node) *ChangeDetectNode {
	return &ChangeDetectNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a ChangeDetect ast.Node
func (n *ChangeDetectNode) Build(d *pipeline.ChangeDetectNode) (ast.Node, error) {
	n.Pipe("changeDetect", d.Field)
	return n.prev, n.err
}
