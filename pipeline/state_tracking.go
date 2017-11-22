package pipeline

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/influxql"
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
	chainnode `json:"-"`

	// Expression to determine whether state is active.
	// tick:ignore
	Lambda *ast.LambdaNode `json:"lambda"`

	// The new name of the resulting duration field.
	// Default: 'state_duration'
	As string `json:"as"`

	// The time unit of the resulting duration value.
	// Default: 1s.
	Unit time.Duration `json:"unit"`
}

func newStateDurationNode(wants EdgeType, predicate *ast.LambdaNode) *StateDurationNode {
	return &StateDurationNode{
		chainnode: newBasicChainNode("state_duration", wants, wants),
		Lambda:    predicate,
		As:        "state_duration",
		Unit:      time.Second,
	}
}

// MarshalJSON converts StateDurationNode to JSON
// tick:ignore
func (n *StateDurationNode) MarshalJSON() ([]byte, error) {
	type Alias StateDurationNode
	var raw = &struct {
		TypeOf
		*Alias
		Unit string `json:"unit"`
	}{
		TypeOf: TypeOf{
			Type: "stateDuration",
			ID:   n.ID(),
		},
		Alias: (*Alias)(n),
		Unit:  influxql.FormatDuration(n.Unit),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an StateDurationNode
// tick:ignore
func (n *StateDurationNode) UnmarshalJSON(data []byte) error {
	type Alias StateDurationNode
	var raw = &struct {
		TypeOf
		*Alias
		Unit string `json:"unit"`
	}{
		Alias: (*Alias)(n),
	}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}
	if raw.Type != "stateDuration" {
		return fmt.Errorf("error unmarshaling node %d of type %s as StateDurationNode", raw.ID, raw.Type)
	}
	n.Unit, err = influxql.ParseDuration(raw.Unit)
	if err != nil {
		return err
	}
	n.setID(raw.ID)
	return nil
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
	chainnode `json:"-"`

	// Expression to determine whether state is active.
	// tick:ignore
	Lambda *ast.LambdaNode `json:"lambda"`

	// The new name of the resulting duration field.
	// Default: 'state_count'
	As string `json:"as"`
}

func newStateCountNode(wants EdgeType, predicate *ast.LambdaNode) *StateCountNode {
	return &StateCountNode{
		chainnode: newBasicChainNode("state_count", wants, wants),
		Lambda:    predicate,
		As:        "state_count",
	}
}

// MarshalJSON converts StateCountNode to JSON
// tick:ignore
func (n *StateCountNode) MarshalJSON() ([]byte, error) {
	type Alias StateCountNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		TypeOf: TypeOf{
			Type: "stateCount",
			ID:   n.ID(),
		},
		Alias: (*Alias)(n),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an StateCountNode
// tick:ignore
func (n *StateCountNode) UnmarshalJSON(data []byte) error {
	type Alias StateCountNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		Alias: (*Alias)(n),
	}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}
	if raw.Type != "stateCount" {
		return fmt.Errorf("error unmarshaling node %d of type %s as StateCountNode", raw.ID, raw.Type)
	}
	n.setID(raw.ID)
	return nil
}
