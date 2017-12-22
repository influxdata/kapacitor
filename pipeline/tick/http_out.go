package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// HTTPOutNode converts the HTTPOutNode pipeline node into the TICKScript AST
type HTTPOutNode struct {
	Function
}

// NewHTTPOut creates a HTTPOutNode function builder
func NewHTTPOut(parents []ast.Node) *HTTPOutNode {
	return &HTTPOutNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a HTTPOutNode ast.Node
func (n *HTTPOutNode) Build(h *pipeline.HTTPOutNode) (ast.Node, error) {
	n.Pipe("httpOut", h.Endpoint)
	return n.prev, n.err
}
