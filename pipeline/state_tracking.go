package pipeline

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

// Compute the duration of a given state.
// The state is defined via a lambda expression. For each consecutive point for
// which the expression evaluates as true, the state duration will be
// incremented by the duration between points. When a point evaluates as false,
// the state duration is reset.
//
// The state duration will be added as an additional field to each point. If the
// expression evaluates as false, the value will be -1. If the expression
// generates an error during evaluation, the point is discarded, and does not
// affect the state duration.
//
// Example:
//     stream
//         |from()
//             .measurement('cpu')
//         |where(lambda: "cpu" == 'cpu-total')
//         |groupBy('host')
//         |stateDuration(lambda: "usage_idle" <= 10)
//             .unit(1m)
//         |alert()
//             // Warn after 1 minute
//             .warn(lambda: "state_duration" >= 1)
//             // Critical after 5 minutes
//             .crit(lambda: "state_duration" >= 5)
//
// Note that as the first point in the given state has no previous point, its
// state duration will be 0.
type StateDurationNode struct {
	chainnode

	// Expression to determine whether state is active.
	// tick:ignore
	Lambda *ast.LambdaNode

	// The new name of the resulting duration field.
	// Default: 'state_duration'
	As string

	// The time unit of the resulting duration value.
	// Default: 1s.
	Unit time.Duration
}

func newStateDurationNode(wants EdgeType, predicate *ast.LambdaNode) *StateDurationNode {
	return &StateDurationNode{
		chainnode: newBasicChainNode("state_duration", wants, wants),
		Lambda:    predicate,
		As:        "state_duration",
		Unit:      time.Second,
	}
}

func (n *StateDurationNode) MarshalJSON() ([]byte, error) {
	props := map[string]interface{}{
		"type":     "stateDuration",
		"nodeID":   fmt.Sprintf("%d", n.ID()),
		"children": n.node,
		"lambda":   n.Lambda,
		"as":       n.As,
		"unit":     n.Unit,
	}
	return json.Marshal(props)
}

// Compute the number of consecutive points in a given state.
// The state is defined via a lambda expression. For each consecutive point for
// which the expression evaluates as true, the state count will be incremented
// When a point evaluates as false, the state count is reset.
//
// The state count will be added as an additional field to each point. If the
// expression evaluates as false, the value will be -1. If the expression
// generates an error during evaluation, the point is discarded, and does not
// affect the state count.
//
// Example:
//     stream
//         |from()
//             .measurement('cpu')
//         |where(lambda: "cpu" == 'cpu-total')
//         |groupBy('host')
//         |stateCount(lambda: "usage_idle" <= 10)
//         |alert()
//             // Warn after 1 point
//             .warn(lambda: "state_count" >= 1)
//             // Critical after 5 points
//             .crit(lambda: "state_count" >= 5)
type StateCountNode struct {
	chainnode

	// Expression to determine whether state is active.
	// tick:ignore
	Lambda *ast.LambdaNode

	// The new name of the resulting duration field.
	// Default: 'state_count'
	As string
}

func newStateCountNode(wants EdgeType, predicate *ast.LambdaNode) *StateCountNode {
	return &StateCountNode{
		chainnode: newBasicChainNode("state_count", wants, wants),
		Lambda:    predicate,
		As:        "state_count",
	}
}

// MarshalJSON converts StateCountNode to JSON
func (n *StateCountNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		SetType("stateCount").
		SetID(n.ID()).
		Set("lambda", n.Lambda).
		Set("as", n.As)

	return json.Marshal(&props)
}

func (n *StateCountNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("stateCount")
	if err != nil {
		return err
	}
	if n.id, err = props.ID(); err != nil {
		return err
	}
	if n.Lambda, err = props.Lambda("lambda"); err != nil {
		return err
	}
	if n.As, err = props.String("as"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON to StateCountNode
func (n *StateCountNode) UnmarshalJSON(data []byte) error {
	props, err := NewJSONNode(data)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}
