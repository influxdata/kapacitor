package tick

import (
	"sort"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// HTTPPostNode converts the HTTPPostNode pipeline node into the TICKScript AST
type HTTPPostNode struct {
	Function
}

// NewHTTPPost creates a HTTPPostNode function builder
func NewHTTPPost(parents []ast.Node) *HTTPPostNode {
	return &HTTPPostNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a HTTPPostNode ast.Node
func (n *HTTPPostNode) Build(h *pipeline.HTTPPostNode) (ast.Node, error) {
	n.Pipe("httpPost", args(h.URLs)...).
		Dot("codeField", h.CodeField).
		DotIf("captureResponse", h.CaptureResponseFlag).
		Dot("timeout", h.Timeout)

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
