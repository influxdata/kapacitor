package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// InfluxQL converts the InfluxQL pipeline node into the TICKScript AST
type InfluxQL struct {
	Function
}

// NewInfluxQL creates a InfluxQL function builder
func NewInfluxQL(parents []ast.Node) *InfluxQL {
	return &InfluxQL{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a InfluxQL ast.Node
func (n *InfluxQL) Build(q *pipeline.InfluxQLNode) (ast.Node, error) {
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
