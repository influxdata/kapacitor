package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// BarrierNode converts the window pipeline node into the TICKScript AST
type BarrierNode struct {
	Function
}

// NewBarrierNode creates a Barrier function builder
func NewBarrierNode(parents []ast.Node) *BarrierNode {
	return &BarrierNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a window ast.Node
func (n *BarrierNode) Build(b *pipeline.BarrierNode) (ast.Node, error) {
	n.Pipe("barrier").
		Dot("idle", b.Idle).
		Dot("period", b.Period).
		Dot("delete", b.Delete)
	return n.prev, n.err
}
