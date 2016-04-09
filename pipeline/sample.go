package pipeline

import (
	"time"
)

// Sample points or batches.
// One point will be emitted every count or duration specified.
//
// Example:
//    stream
//        |sample(3)
//
// Keep every third data point or batch.
//
// Example:
//    stream
//        |sample(10s)
//
// Keep only samples that land on the 10s boundary.
// See FromNode.Truncate, QueryNode.GroupBy time or WindowNode.Align
// for ensuring data is aligned with a boundary.
type SampleNode struct {
	chainnode

	// Keep every N point or batch
	// tick:ignore
	N int64

	// Keep one point or batch every Duration
	// tick:ignore
	Duration time.Duration
}

func newSampleNode(wants EdgeType, rate interface{}) *SampleNode {
	var n int64
	var d time.Duration
	switch r := rate.(type) {
	case int64:
		n = r
	case time.Duration:
		d = r
	default:
		panic("must pass int64 or duration to new sample node")
	}

	return &SampleNode{
		chainnode: newBasicChainNode("sample", wants, wants),
		N:         n,
		Duration:  d,
	}
}
