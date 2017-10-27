package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// InfluxQLNode converts the InfluxQLNode pipeline node into the TICKScript AST
type InfluxQLNode struct {
	Function
}

// NewInfluxQL creates a InfluxQLNode function builder
func NewInfluxQL(parents []ast.Node) *InfluxQLNode {
	return &InfluxQLNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a InfluxQLNode ast.Node
func (n *InfluxQLNode) Build(q *pipeline.InfluxQLNode) (ast.Node, error) {
	args := []interface{}{}
	if q.Field != "" {
		args = append(args, q.Field)
	}
	args = append(args, q.Args...)
	n.Pipe(q.Method, args...).
		Dot("as", q.As).
		DotIf("usePointTimes", q.PointTimes)
	return n.prev, n.err
}
