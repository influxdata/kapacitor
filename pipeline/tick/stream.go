package tick

import (
	"github.com/influxdata/kapacitor/tick/ast"
)

// Stream converts the stream pipeline node into the TICKScript AST
type Stream struct{}

// Build stream ast.Node
func (s *Stream) Build() (ast.Node, error) {
	return &ast.IdentifierNode{
		Ident: "stream",
	}, nil
}
