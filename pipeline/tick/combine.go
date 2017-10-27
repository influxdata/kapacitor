package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// CombineNode converts the Combine pipeline node into the TICKScript AST
type CombineNode struct {
	Function
}

// NewCombine creates a Combine function builder
func NewCombine(parents []ast.Node) *CombineNode {
	return &CombineNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a Combine ast.Node
func (n *CombineNode) Build(c *pipeline.CombineNode) (ast.Node, error) {
	n.Pipe("combine", largs(c.Lambdas)...).
		Dot("as", args(c.Names)...).
		Dot("delimiter", c.Delimiter).
		Dot("tolerance", c.Tolerance).
		Dot("max", c.Max)

	return n.prev, n.err
}
