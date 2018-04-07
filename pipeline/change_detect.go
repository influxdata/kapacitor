package pipeline

import (
	"encoding/json"
	"fmt"
)

// Compute the changeDetect of a stream or batch.
// The changeDetect is computed on a single field,
// discarding consecutive duplicate values, detecting
// detects the points at which the series field
// changes from one value to another.
//
//
// Example:
//     stream
//         |from()
//             .measurement('packets')
//         |changeDetect('value')
//         ...
//
// with source data:
// packets value="bad" 0000000000
// packets value="good" 0000000001
// packets value="bad" 0000000002
// packets value="bad" 0000000003
// packets value="bad" 0000000004
// packets value="good" 0000000005
//
// Would have output:
// packets value="bad" 0000000000
// packets value="good" 0000000001
// packets value="bad" 0000000002
// packets value="good" 0000000005
//
// Where the data are unchanged, but only the points
// where the value changes from the previous value are
// emitted.

type ChangeDetectNode struct {
	chainnode `json:"-"`

	// The field to use when calculating the changeDetect
	// tick:ignore
	Field string `json:"field"`
}

func newChangeDetectNode(wants EdgeType, field string) *ChangeDetectNode {
	return &ChangeDetectNode{
		chainnode: newBasicChainNode("changeDetect", wants, wants),
		Field:     field,
	}
}

// MarshalJSON converts ChangeDetectNode to JSON
// tick:ignore
func (n *ChangeDetectNode) MarshalJSON() ([]byte, error) {
	type Alias ChangeDetectNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		TypeOf: TypeOf{
			Type: "changeDetect",
			ID:   n.ID(),
		},
		Alias: (*Alias)(n),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an ChangeDetectNode
// tick:ignore
func (n *ChangeDetectNode) UnmarshalJSON(data []byte) error {
	type Alias ChangeDetectNode
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
	if raw.Type != "changeDetect" {
		return fmt.Errorf("error unmarshaling node %d of type %s as ChangeDetectNode", raw.ID, raw.Type)
	}

	n.setID(raw.ID)
	return nil
}
