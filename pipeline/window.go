package pipeline

import (
	"time"
)

// Windows data over time. A window of length Period is emitted into the pipeline every Every durations.
type WindowNode struct {
	node
	// The period, or length in time, of the window.
	Period time.Duration
	// How often the current window is emitted into the pipeline.
	Every time.Duration
}

func newWindowNode() *WindowNode {
	return &WindowNode{
		node: node{
			desc:     "window",
			wants:    StreamEdge,
			provides: BatchEdge,
		},
	}
}
