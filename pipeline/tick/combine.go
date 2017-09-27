package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Combine converts the Combine pipeline node into the TICKScript AST
type Combine struct {
	Function
}

// Build creates a Combine ast.Node
func (n *Combine) Build(c *pipeline.CombineNode) (ast.Node, error) {
	n.Pipe("combine", c.Lambdas...).
		Dot("as", c.Names...).
		Dot("delimiter", c.Delimiter).
		Dot("tolerance", c.Tolerance).
		Dot("max", db.Max)

	return n.prev, n.err
}
