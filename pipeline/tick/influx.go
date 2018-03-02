package tick

import (
	"strings"

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
	if strings.ToLower(q.Method) == "bottom" || strings.ToLower(q.Method) == "top" {
		if len(q.Args) > 0 {
			args = append(args, q.Args[0])
			args = append(args, q.Field)
			args = append(args, q.Args[1:]...)
		}
	} else {
		if q.Field != "" {
			args = append(args, q.Field)
		}
		args = append(args, q.Args...)
	}
	n.Pipe(q.Method, args...).
		Dot("as", q.As).
		DotIf("usePointTimes", q.PointTimes)
	return n.prev, n.err
}
