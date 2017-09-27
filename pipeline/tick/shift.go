package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Shift converts the shift pipeline node into the TICKScript AST
type Shift struct {
	Function
}

// NewShift creates a Shift function builder
func NewShift(parents []ast.Node) *Shift {
	return &Shift{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a Shift ast.Node
func (n *Shift) Build(s *pipeline.ShiftNode) (ast.Node, error) {
	n.Pipe("shift", s.Shift)
	return n.prev, n.err
}
