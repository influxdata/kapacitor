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
//    * collected -- the number of points or batches this node has received.
//
// Each stat is available as a field in the emitted data stream.
//
// Example:
//     var data = stream.from()...
//     // Emit statistics every 1 minute and cache them via the HTTP API.
//     data.stats(1m).httpOut('stats')
//     // Contiue normal processing of the data stream
//     data....
//
// WARNING: It is not recommened to join the stats stream with the orginal data stream.
// Since they operate on different clocks you could potentially create a deadlock.
// This is a limitation of the current implementation and may be removed in the future.
type StatsNode struct {
	chainnode
	// tick:ignore
	SourceNode Node
	// tick:ignore
	Interval time.Duration
}

func newStatsNode(n Node, interval time.Duration) *StatsNode {
	return &StatsNode{
		chainnode:  newBasicChainNode("stats", StreamEdge, StreamEdge),
		SourceNode: n,
		Interval:   interval,
	}
}
