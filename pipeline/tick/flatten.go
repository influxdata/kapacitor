package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Flatten converts the Flatten pipeline node into the TICKScript AST
type Flatten struct {
	Function
}

// Build creates a Flatten ast.Node
func (n *Flatten) Build(f *pipeline.FlattenNode) (ast.Node, error) {
	n.Pipe("flatten").
		Dot("on", f.Dimensions...).
		Dot("delimiter", f.Delimiter).
		Dot("tolerance", f.Tolerance).
		DotIf("dropOriginalFieldName", f.DropOriginalFieldNameFlag)

	return n.prev, n.err
}
