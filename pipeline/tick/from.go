package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// From converts the from pipeline node into the TICKScript AST
type From struct {
	Function
}

// Build creates a from ast.Node
func (n *From) Build(f *pipeline.FromNode) (ast.Node, error) {
	n.Pipe("from").
		Dot("database", f.Database).
		Dot("retentionPolicy", f.RetentionPolicy).
		Dot("measurement", f.Measurement).
		DotIf("groupByMeasurement", f.GroupByMeasurementFlag).
		Dot("round", f.Round).
		Dot("groupBy", f.Dimensions).
		Dot("truncate", f.Truncate).
		Dot("where", f.Lambda)
	return n.prev, n.err
}
