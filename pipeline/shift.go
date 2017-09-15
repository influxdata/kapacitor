package pipeline

import (
	"encoding/json"
	"fmt"
	"time"
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
	chainnode

	// Keep one point or batch every Duration
	// tick:ignore
	Shift time.Duration
}

func newShiftNode(wants EdgeType, shift time.Duration) *ShiftNode {
	return &ShiftNode{
		chainnode: newBasicChainNode("shift", wants, wants),
		Shift:     shift,
	}
}

func (n *ShiftNode) MarshalJSON() ([]byte, error) {
	props := map[string]interface{}{
		"type":     "shift",
		"nodeID":   fmt.Sprintf("%d", n.ID()),
		"children": n.node,
		"shift":    n.Shift,
	}
	return json.Marshal(props)
}
