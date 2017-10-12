package pipeline

import (
	"encoding/json"
	"time"
)

// A StatsNode emits internal statistics about the another node at a given interval.
//
// The interval represents how often to emit the statistics based on real time.
// This means the interval time is independent of the times of the data points the other node is receiving.
// As a result the StatsNode is a root node in the task pipeline.
//
//
// The currently available internal statistics:
//
//    * emitted -- the number of points or batches this node has sent to its children.
//
// Each stat is available as a field in the data stream.
//
// The stats are in groups according to the original data.
// Meaning that if the source node is grouped by the tag 'host' as an example,
// then the counts are output per host with the appropriate 'host' tag.
// Since its possible for groups to change when crossing a node only the emitted groups
// are considered.
//
// Example:
//     var data = stream
//         |from()...
//     // Emit statistics every 1 minute and cache them via the HTTP API.
//     data
//         |stats(1m)
//         |httpOut('stats')
//     // Continue normal processing of the data stream
//     data...
//
// WARNING: It is not recommended to join the stats stream with the original data stream.
// Since they operate on different clocks you could potentially create a deadlock.
// This is a limitation of the current implementation and may be removed in the future.
type StatsNode struct {
	chainnode
	// tick:ignore
	SourceNode Node
	// tick:ignore
	Interval time.Duration

	// tick:ignore
	AlignFlag bool `tick:"Align"`
}

func newStatsNode(n Node, interval time.Duration) *StatsNode {
	return &StatsNode{
		chainnode:  newBasicChainNode("stats", StreamEdge, StreamEdge),
		SourceNode: n,
		Interval:   interval,
	}
}

// MarshalJSON converts StatsNode to JSON
func (n *StatsNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		SetType("stats").
		SetID(n.ID()).
		SetDuration("interval", n.Interval).
		Set("align", n.AlignFlag)

	return json.Marshal(&props)
}

func (n *StatsNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("stats")
	if err != nil {
		return err
	}
	if n.id, err = props.ID(); err != nil {
		return err
	}
	if n.Interval, err = props.Duration("interval"); err != nil {
		return err
	}
	if n.AlignFlag, err = props.Bool("align"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON to StatsNode
func (n *StatsNode) UnmarshalJSON(data []byte) error {
	props, err := NewJSONNode(data)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

// Round times to the StatsNode.Interval value.
// tick:property
func (n *StatsNode) Align() *StatsNode {
	n.AlignFlag = true
	return n
}
