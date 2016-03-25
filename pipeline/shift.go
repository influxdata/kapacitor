package pipeline

import (
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
