package tick

import "github.com/influxdata/kapacitor/tick/ast"

// Batch converts the batch pipeline node into the TICKScript AST
type Batch struct{}

// Build batch ast.Node
func (b *Batch) Build() (ast.Node, error) {
	return &ast.IdentifierNode{
		Ident: "batch",
	}, nil
}
