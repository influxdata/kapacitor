package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// ShiftNode converts the ShiftNode pipeline node into the TICKScript AST
type ShiftNode struct {
	Function
}

// NewShift creates a ShiftNode function builder
func NewShift(parents []ast.Node) *ShiftNode {
	return &ShiftNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a ShiftNode ast.Node
func (n *ShiftNode) Build(s *pipeline.ShiftNode) (ast.Node, error) {
	n.Pipe("shift", s.Shift)
	return n.prev, n.err
}
