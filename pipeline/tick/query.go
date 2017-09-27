package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Query converts the Query pipeline node into the TICKScript AST
type Query struct {
	Function
}

// Build creates a Query ast.Node
func (n *Query) Build(q *pipeline.QueryNode) (ast.Node, error) {
	n.Pipe("query", q.QueryStr).
		Dot("period", q.Period).
		Dot("every", q.Every).
		DotIf("align", q.AlignFlag).
		Dot("cron", q.Cron).
		Dot("offset", q.Offset).
		DotIf("alignGroup", q.AlignGroupFlag).
		Dot("groupBy", q.Dimentions...).
		DotIf("groupByMeasurement", q.GroupByMeasurementFlag).
		Dot("fill", q.Fill).
		Dot("cluster", q.Cluster)

	return n.prev, n.err
}
