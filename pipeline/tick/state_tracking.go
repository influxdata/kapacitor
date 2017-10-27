package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// StateDurationNode converts the StateDurationNode pipeline node into the TICKScript AST
type StateDurationNode struct {
	Function
}

// NewStateDuration creates a StateDurationNode function builder
func NewStateDuration(parents []ast.Node) *StateDurationNode {
	return &StateDurationNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a StateDurationNode ast.Node
func (n *StateDurationNode) Build(s *pipeline.StateDurationNode) (ast.Node, error) {
	n.Pipe("stateDuration", s.Lambda).
		Dot("as", s.As).
		Dot("unit", s.Unit)

	return n.prev, n.err
}

// StateCountNode converts the StateCountNode pipeline node into the TICKScript AST
type StateCountNode struct {
	Function
}

// NewStateCount creates a StateCountNode function builder
func NewStateCount(parents []ast.Node) *StateCountNode {
	return &StateCountNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a StateCountNode ast.Node
func (n *StateCountNode) Build(s *pipeline.StateCountNode) (ast.Node, error) {
	n.Pipe("stateCount", s.Lambda).
		Dot("as", s.As)

	return n.prev, n.err
}
