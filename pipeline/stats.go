package pipeline

import "time"

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

// Round times to the StatsNode.Interval value.
// tick:property
func (n *StatsNode) Align() *StatsNode {
	n.AlignFlag = true
	return n
}
