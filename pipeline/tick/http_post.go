package tick

import (
	"sort"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// HTTPPost converts the HTTPPost pipeline node into the TICKScript AST
type HTTPPost struct {
	Function
}

// NewHTTPPost creates a HTTPPost function builder
func NewHTTPPost(parents []ast.Node) *HTTPPost {
	return &HTTPPost{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a HTTPPost ast.Node
func (n *HTTPPost) Build(h *pipeline.HTTPPostNode) (ast.Node, error) {
	n.Pipe("httpPost", args(h.URLs))
	for _, e := range h.Endpoints {
		n.Dot("endpoint", e)
	}

	var headers []string
	for k := range h.Headers {
		headers = append(headers, k)
	}
	sort.Strings(headers)
	for _, k := range headers {
		n.Dot("header", k, h.Headers[k])
	}

	return n.prev, n.err
}
