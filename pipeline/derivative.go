package pipeline

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/influxql"
)

// Compute the derivative of a stream or batch.
// The derivative is computed on a single field
// and behaves similarly to the InfluxQL derivative
// function. Kapacitor has its own implementation
// of the derivative function, and, as a result, is
// not part of the normal InfluxQL functions.
//
// Example:
//     stream
//         |from()
//             .measurement('net_rx_packets')
//         |derivative('value')
//            .unit(1s) // default
//            .nonNegative()
//         ...
//
// Computes the derivative via:
//    (current - previous ) / ( time_difference / unit)
//
// The derivative is computed for each point, and
// because of boundary conditions the first point is
// dropped.
type DerivativeNode struct {
	chainnode `json:"-"`

	// The field to use when calculating the derivative
	// tick:ignore
	Field string `json:"field"`

	// The new name of the derivative field.
	// Default is the name of the field used
	// when calculating the derivative.
	As string `json:"as"`

	// The time unit of the resulting derivative value.
	// Default: 1s
	Unit time.Duration `json:"unit"`

	// Where negative values are acceptable.
	// tick:ignore
	NonNegativeFlag bool `tick:"NonNegative" json:"nonNegative"`
}

func newDerivativeNode(wants EdgeType, field string) *DerivativeNode {
	return &DerivativeNode{
		chainnode: newBasicChainNode("derivative", wants, wants),
		Unit:      time.Second,
		Field:     field,
		As:        field,
	}
}

// MarshalJSON converts DerivativeNode to JSON
// tick:ignore
func (n *DerivativeNode) MarshalJSON() ([]byte, error) {
	type Alias DerivativeNode
	var raw = &struct {
		TypeOf
		*Alias
		Unit string `json:"unit"`
	}{
		TypeOf: TypeOf{
			Type: "derivative",
			ID:   n.ID(),
		},
		Alias: (*Alias)(n),
		Unit:  influxql.FormatDuration(n.Unit),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an DerivativeNode
// tick:ignore
func (n *DerivativeNode) UnmarshalJSON(data []byte) error {
	type Alias DerivativeNode
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
	if raw.Type != "derivative" {
		return fmt.Errorf("error unmarshaling node %d of type %s as DerivativeNode", raw.ID, raw.Type)
	}
	n.Unit, err = influxql.ParseDuration(raw.Unit)
	if err != nil {
		return err
	}
	n.setID(raw.ID)
	return nil
}

// If called the derivative will skip negative results.
// tick:property
func (d *DerivativeNode) NonNegative() *DerivativeNode {
	d.NonNegativeFlag = true
	return d
}
