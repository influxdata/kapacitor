package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// InfluxQL converts the InfluxQL pipeline node into the TICKScript AST
type InfluxQL struct {
	Function
}

// Build creates a InfluxQL ast.Node
func (n *InfluxQL) Build(q *pipeline.InfluxQLNode) (ast.Node, error) {
	// TODO: ReduceCreater?
	n.Pipe(q.Method, q.Field).
		Dot("as", q.As).
		DotIf("usePointTimes", q.PointTimes)
	return n.prev, n.err
}
