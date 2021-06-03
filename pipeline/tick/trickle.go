package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// TrickleNode converts the StatsNode pipeline node into the TICKScript AST
type TrickleNode struct {
	Function
}

// NewTrickle creates a TrickleNode function builder
func NewTrickle(parents []ast.Node) *TrickleNode {
	return &TrickleNode{
		Function{
			Parents: parents,
		},
	}
}

// Build NewTrickle ast.Node
func (n *TrickleNode) Build(s *pipeline.TrickleNode) (ast.Node, error) {
	n.Pipe("trickle")
	return n.prev, n.err
}
