package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Stream converts the stream pipeline node into the TICKScript AST
func (a *AST) Stream() *AST {
	a.Node = &ast.IdentifierNode{
		Ident: "stream",
	}
	return a
}

// StreamFrom converts the from pipeline node into the TICKScript AST
func (a *AST) StreamFrom(f *pipeline.FromNode) *AST {
	// TODO: Handle the errors
	a.Stream()
	from, _ := PipeFunction(a.Node, "from")
	from, _ = DotFunction(from, "database", f.Database)
	from, _ = DotFunction(from, "retentionPolicy", f.RetentionPolicy)
	from, _ = DotFunction(from, "measurement", f.Measurement)
	from, _ = DotFunctionIf(from, "groupByMeasurement", f.GroupByMeasurementFlag)
	from, _ = DotFunction(from, "round", f.Round)
	from, _ = DotFunction(from, "groupBy", f.Dimensions)
	from, _ = DotFunction(from, "truncate", f.Truncate)
	from, _ = DotFunction(from, "where", f.Lambda)
	a.Node = from
	return a
}
