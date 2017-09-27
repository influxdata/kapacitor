package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// HTTPPost converts the HTTPPost pipeline node into the TICKScript AST
type HTTPPost struct {
	Function
}

// Build creates a HTTPPost ast.Node
func (n *HTTPPost) Build(h *pipeline.HTTPPostNode) (ast.Node, error) {
	n.Pipe("httpPost", h.URLs...)
	for _, e := range h.Endpoints {
		n.Dot("endpoint", e)
	}
	for k, v := range h.Headers {
		n.Dot("header", k, v)
	}

	return n.prev, n.err
}
