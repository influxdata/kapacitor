package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// QueryNode converts the QueryNode pipeline node into the TICKScript AST
type QueryNode struct {
	Function
}

// NewQuery creates a QueryNode function builder
func NewQuery(parents []ast.Node) *QueryNode {
	return &QueryNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a QueryNode ast.Node
func (n *QueryNode) Build(q *pipeline.QueryNode) (ast.Node, error) {
	n.Pipe("query", q.QueryStr).
		Dot("period", q.Period).
		Dot("every", q.Every).
		DotIf("align", q.AlignFlag).
		Dot("cron", q.Cron).
		Dot("offset", q.Offset).
		DotIf("alignGroup", q.AlignGroupFlag).
		Dot("groupBy", q.Dimensions).
		DotIf("groupByMeasurement", q.GroupByMeasurementFlag).
		DotNotNil("fill", q.Fill).
		Dot("cluster", q.Cluster)

	return n.prev, n.err
}
