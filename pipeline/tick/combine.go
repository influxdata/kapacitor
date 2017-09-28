package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Combine converts the Combine pipeline node into the TICKScript AST
type Combine struct {
	Function
}

// NewCombine creates a Combine function builder
func NewCombine(parents []ast.Node) *Combine {
	return &Combine{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a Combine ast.Node
func (n *Combine) Build(c *pipeline.CombineNode) (ast.Node, error) {
	n.Pipe("combine", largs(c.Lambdas)...).
		Dot("as", args(c.Names)...).
		Dot("delimiter", c.Delimiter).
		Dot("tolerance", c.Tolerance).
		Dot("max", c.Max)

	return n.prev, n.err
}
