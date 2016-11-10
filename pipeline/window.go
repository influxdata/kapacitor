package pipeline

import (
	"errors"
	"fmt"
	"time"
)

// A `window` node caches data within a moving time range.
// The `period` property of `window` defines the time range covered by `window`.
//
// The `every` property of `window` defines the frequency at which the window
// is emitted to the next node in the pipeline.
//
//The `align` property of `window` defines how to align the window edges.
//(By default, the edges are defined relative to the first data point the `window`
//node receives.)
//
// Example:
//    stream
//        |window()
//            .period(10m)
//            .every(5m)
//        |httpOut('recent')
//
// his example emits the last `10 minute` period  every `5 minutes` to the pipeline's `httpOut` node.
// Because `every` is less than `period`, each time the window is emitted it contains `5 minutes` of
// new data and `5 minutes` of the previous period's data.
//
// NOTE: Because no `align` property is defined, the `window` edge is defined relative to the first data point.
type WindowNode struct {
	chainnode
	// The period, or length in time, of the window.
	Period time.Duration
	// How often the current window is emitted into the pipeline.
	// If equal to zero, then every new point will emit the current window.
	Every time.Duration
	// Whether to align the window edges with the zero time
	// tick:ignore
	AlignFlag bool `tick:"Align"`
	// Whether to wait till the period is full before the first emit.
	// tick:ignore
	FillPeriodFlag bool `tick:"FillPeriod"`

	// PeriodCount is the number of points per window.
	PeriodCount int64
	// EveryCount determines how often the window is emitted based on the count of points.
	// A value of 1 means that every new point will emit the window.
	EveryCount int64
}

func newWindowNode() *WindowNode {
	return &WindowNode{
		chainnode: newBasicChainNode("window", StreamEdge, BatchEdge),
	}
}

// If the `align` property is not used to modify the `window` node, then the
// window alignment is assumed to start at the time of the first data point it receives.
// If `align` property is set, the window time edges
// will be truncated to the `every` property (For example, if a data point's time
// is 12:06 and the `every` property is `5m` then the data point's window will range
// from 12:05 to 12:10).
// tick:property
func (w *WindowNode) Align() *WindowNode {
	w.AlignFlag = true
	return w
}

// FillPeriod instructs the WindowNode to wait till the period has elapsed before emitting the first batch.
// This only applies if the period is greater than the every value.
// tick:property
func (w *WindowNode) FillPeriod() *WindowNode {
	w.FillPeriodFlag = true
	return w
}

func (w *WindowNode) validate() error {
	if w.PeriodCount != 0 && w.Period != 0 {
		return errors.New("cannot specify both period and periodCount")
	}
	if w.PeriodCount != 0 && w.AlignFlag {
		return errors.New("can only align windows based off time, not count")
	}
	if w.PeriodCount != 0 && w.EveryCount <= 0 {
		return fmt.Errorf("everyCount must be greater than zero")
	}
	return nil
}
