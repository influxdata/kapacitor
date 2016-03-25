package pipeline

import (
	"time"
)

// Windows data over time.
// A window has a length defined by `period`
// and a frequency at which it emits the window to the pipeline.
//
// Example:
//    stream
//        |window()
//            .period(10m)
//            .every(5m)
//        |httpOut('recent')
//
// The above windowing example emits a window to the pipeline every `5 minutes`
// and the window contains the last `10 minutes` worth of data.
// As a result each time the window is emitted it contains half new data and half old data.
//
// NOTE: Time for a window (or any node) is implemented by inspecting the times on the incoming data points.
// As a result if the incoming data stream stops then no more windows will be emitted because time is no longer
// increasing for the window node.
type WindowNode struct {
	chainnode
	// The period, or length in time, of the window.
	Period time.Duration
	// How often the current window is emitted into the pipeline.
	Every time.Duration
	// Wether to align the window edges with the zero time
	// tick:ignore
	AlignFlag bool `tick:"Align"`
}

func newWindowNode() *WindowNode {
	return &WindowNode{
		chainnode: newBasicChainNode("window", StreamEdge, BatchEdge),
	}
}

// Wether to align the window edges with the zero time.
// If not aligned the window starts and ends relative to the
// first data point it receives.
// tick:property
func (w *WindowNode) Align() *WindowNode {
	w.AlignFlag = true
	return w
}
