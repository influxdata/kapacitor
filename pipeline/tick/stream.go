package tick

import (
	"github.com/influxdata/kapacitor/tick/ast"
)

// StreamNode converts the stream pipeline node into the TICKScript AST
type StreamNode struct{}

// Build stream ast.Node
func (s *StreamNode) Build() (ast.Node, error) {
	return &ast.IdentifierNode{
		Ident: "stream",
	}, nil
}
