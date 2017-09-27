package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// StateDuration converts the StateDuration pipeline node into the TICKScript AST
type StateDuration struct {
	Function
}

// NewStateDuration creates a StateDuration function builder
func NewStateDuration(parents []ast.Node) *StateDuration {
	return &StateDuration{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a StateDuration ast.Node
func (n *StateDuration) Build(s *pipeline.StateDurationNode) (ast.Node, error) {
	n.Pipe("stateDuration", s.Lambda).
		Dot("as", s.As).
		Dot("unit", s.Unit)

	return n.prev, n.err
}

// StateCount converts the StateCount pipeline node into the TICKScript AST
type StateCount struct {
	Function
}

// NewStateCount creates a StateCount function builder
func NewStateCount(parents []ast.Node) *StateCount {
	return &StateCount{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a StateCount ast.Node
func (n *StateCount) Build(s *pipeline.StateCountNode) (ast.Node, error) {
	n.Pipe("stateCount", s.Lambda).
		Dot("as", s.As)

	return n.prev, n.err
}
