package pipeline

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/influxql"
)

// Shift points and batches in time, this is useful for comparing
// batches or points from different times.
//
// Example:
//    stream
//        |shift(5m)
//
// Shift all data points 5m forward in time.
//
// Example:
//    stream
//        |shift(-10s)
//
// Shift all data points 10s backward in time.
type ShiftNode struct {
	chainnode `json:"-"`

	// Keep one point or batch every Duration
	// tick:ignore
	Shift time.Duration `json:"shift"`
}

func newShiftNode(wants EdgeType, shift time.Duration) *ShiftNode {
	return &ShiftNode{
		chainnode: newBasicChainNode("shift", wants, wants),
		Shift:     shift,
	}
}

// MarshalJSON converts ShiftNode to JSON
// tick:ignore
func (n *ShiftNode) MarshalJSON() ([]byte, error) {
	type Alias ShiftNode
	var raw = &struct {
		TypeOf
		*Alias
		Shift string `json:"shift"`
	}{
		TypeOf: TypeOf{
			Type: "shift",
			ID:   n.ID(),
		},
		Alias: (*Alias)(n),
		Shift: influxql.FormatDuration(n.Shift),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an ShiftNode
// tick:ignore
func (n *ShiftNode) UnmarshalJSON(data []byte) error {
	type Alias ShiftNode
	var raw = &struct {
		TypeOf
		*Alias
		Shift string `json:"shift"`
	}{
		Alias: (*Alias)(n),
	}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}
	if raw.Type != "shift" {
		return fmt.Errorf("error unmarshaling node %d of type %s as ShiftNode", raw.ID, raw.Type)
	}
	n.Shift, err = influxql.ParseDuration(raw.Shift)
	if err != nil {
		return err
	}
	n.setID(raw.ID)
	return nil
}
