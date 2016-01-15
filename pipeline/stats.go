package pipeline

import "time"

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
