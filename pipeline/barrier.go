package pipeline

import (
	"errors"
	"time"
)

// A BarrierNode will emit a barrier with the current time, according to the system
// clock.  Since the BarrierNode emits based on system time, it allows pipelines to be
// forced in the absence of data traffic.  The barrier emitted will be based on either
// idle time since the last received message or on a periodic timer based on the system
// clock.  Any messages received after an emitted barrier that is older than the last
// emitted barrier will be dropped.
//
// Example:
//    stream
//        |barrier().idle(5s)
//        |window()
//            .period(10s)
//            .every(5s)
//        |top(10, 'value')
//        //Post the top 10 results over the last 10s updated every 5s.
//        |httpPost('http://example.com/api/top10')
//
type BarrierNode struct {
	chainnode

	// Emit barrier based on idle time since the last received message.
	// Must be greater than zero.
	Idle time.Duration

	// Emit barrier based on periodic timer.  The timer is based on system
	// clock rather than message time.
	// Must be greater than zero.
	Period time.Duration
}

func newBarrierNode(wants EdgeType) *BarrierNode {
	return &BarrierNode{
		chainnode: newBasicChainNode("barrier", wants, wants),
	}
}

// tick:ignore
func (b *BarrierNode) validate() error {
	if b.Idle != 0 && b.Period != 0 {
		return errors.New("cannot specify both idle and period")
	}
	if b.Period == 0 && b.Idle <= 0 {
		return errors.New("idle must be greater than zero")
	}
	if b.Period <= 0 && b.Idle == 0 {
		return errors.New("period must be greater than zero")
	}

	return nil
}
