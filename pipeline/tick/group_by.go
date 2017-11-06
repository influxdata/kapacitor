package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// GroupByNode converts the GroupByNode pipeline node into the TICKScript AST
type GroupByNode struct {
	Function
}

// NewGroupBy creates a GroupByNode function builder
func NewGroupBy(parents []ast.Node) *GroupByNode {
	return &GroupByNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a GroupByNode ast.Node
func (n *GroupByNode) Build(g *pipeline.GroupByNode) (ast.Node, error) {
	n.Pipe("groupBy", g.Dimensions...).
		Dot("exclude", args(g.ExcludedDimensions)...).
		DotIf("byMeasurement", g.ByMeasurementFlag)

	return n.prev, n.err
}
