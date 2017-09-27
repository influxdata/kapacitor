package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// HTTPOut converts the HTTPOut pipeline node into the TICKScript AST
type HTTPOut struct {
	Function
}

// Build creates a HTTPOut ast.Node
func (n *HTTPOut) Build(h *pipeline.HTTPOutNode) (ast.Node, error) {
	n.Pipe("httpOut", h.Endpoint)
	return n.prev, n.err
}
