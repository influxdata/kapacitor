package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// QueryFluxNode converts the QueryFluxNode pipeline node into the TICKScript AST
type QueryFluxNode struct {
	Function
}

// NewQueryFlux creates a QueryNode function builder
func NewQueryFlux(parents []ast.Node) *QueryFluxNode {
	return &QueryFluxNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a QueryNode ast.Node
func (n *QueryFluxNode) Build(q *pipeline.QueryFluxNode) (ast.Node, error) {
	n.Pipe("queryFlux", q.QueryStr).
		Dot("period", q.Period).
		Dot("every", q.Every).
		DotIf("align", q.AlignFlag).
		Dot("cron", q.Cron).
		Dot("offset", q.Offset).
		Dot("cluster", q.Cluster).
		Dot("orgID", q.OrgID).
		Dot("org", q.Org)
	return n.prev, n.err
}
