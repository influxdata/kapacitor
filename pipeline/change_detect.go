package pipeline

import (
	"encoding/json"
	"fmt"
)

// Compute the changeDetect of a stream or batch.
// The changeDetect is computed on a single field
// and behaves similarly to the InfluxQL changeDetect
// function. Kapacitor has its own implementation
// of the changeDetect function, and, as a result, is
// not part of the normal InfluxQL functions.
//
// Example:
//     stream
//         |from()
//             .measurement('net_rx_packets')
//         |changeDetect('value')
//            .unit(1s) // default
//            .nonNegative()
//         ...
//
// Computes the changeDetect via:
//    (current - previous ) / ( time_difference / unit)
//
// The changeDetect is computed for each point, and
// because of boundary conditions the first point is
// dropped.
type ChangeDetectNode struct {
	chainnode `json:"-"`

	// The field to use when calculating the changeDetect
	// tick:ignore
	Field string `json:"field"`

	// The new name of the changeDetect field.
	// Default is the name of the field used
	// when calculating the changeDetect.
	As string `json:"as"`
}

func newChangeDetectNode(wants EdgeType, field string) *ChangeDetectNode {
	return &ChangeDetectNode{
		chainnode: newBasicChainNode("changeDetect", wants, wants),
		Field:     field,
		As:        field,
	}
}

// MarshalJSON converts ChangeDetectNode to JSON
// tick:ignore
func (n *ChangeDetectNode) MarshalJSON() ([]byte, error) {
	type Alias ChangeDetectNode
	var raw = &struct {
		TypeOf
		*Alias
		Unit string `json:"unit"`
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
		Unit string `json:"unit"`
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
