package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// FlattenNode converts the FlattenNode pipeline node into the TICKScript AST
type FlattenNode struct {
	Function
}

// NewFlatten creates a FlattenNode function builder
func NewFlatten(parents []ast.Node) *FlattenNode {
	return &FlattenNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a Flatten ast.Node
func (n *FlattenNode) Build(f *pipeline.FlattenNode) (ast.Node, error) {
	n.Pipe("flatten").
		Dot("on", args(f.Dimensions)...).
		Dot("delimiter", f.Delimiter).
		Dot("tolerance", f.Tolerance).
		DotIf("dropOriginalFieldName", f.DropOriginalFieldNameFlag)

	return n.prev, n.err
}
