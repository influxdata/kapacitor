package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Derivative converts the Derivative pipeline node into the TICKScript AST
type Derivative struct {
	Function
}

// NewDerivative creates a Derivative function builder
func NewDerivative(parents []ast.Node) *Derivative {
	return &Derivative{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a Derivative ast.Node
func (n *Derivative) Build(d *pipeline.DerivativeNode) (ast.Node, error) {
	n.Pipe("derivative", d.Field).
		Dot("as", d.As).
		Dot("unit", d.Unit).
		DotIf("nonNegative", d.NonNegativeFlag)
	return n.prev, n.err
}
