package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// GroupBy converts the GroupBy pipeline node into the TICKScript AST
type GroupBy struct {
	Function
}

// Build creates a GroupBy ast.Node
func (n *GroupBy) Build(g *pipeline.GroupByNode) (ast.Node, error) {
	n.Pipe("groupBy", g.Dimensions...).
		Dot("exclude", g.ExcludedDimensions...).
		DotIf("byMeasurement", g.ByMeasurementFlag)

	return n.prev, n.err
}
