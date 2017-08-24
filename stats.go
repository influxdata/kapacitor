package kapacitor

import (
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/pipeline"
)

type StatsNode struct {
	node
	s       *pipeline.StatsNode
	en      Node
	closing chan struct{}
	closed  bool
	mu      sync.Mutex
}

// Create a new  FromNode which filters data from a source.
func newStatsNode(et *ExecutingTask, n *pipeline.StatsNode, d NodeDiagnostic) (*StatsNode, error) {
	// Lookup the executing node for stats.
	en := et.lookup[n.SourceNode.ID()]
	if en == nil {
		return nil, fmt.Errorf("no node found for %s", n.SourceNode.Name())
	}
	sn := &StatsNode{
		node:    node{Node: n, et: et, diag: d},
		s:       n,
		en:      en,
		closing: make(chan struct{}),
	}
	sn.node.runF = sn.runStats
	sn.node.stopF = sn.stopStats
	return sn, nil
}

func (n *StatsNode) runStats([]byte) error {
	if n.s.AlignFlag {
		// Wait till we are roughly aligned with the interval.
		now := time.Now()
		next := now.Truncate(n.s.Interval).Add(n.s.Interval)
		after := time.NewTicker(next.Sub(now))
		select {
		case <-after.C:
			after.Stop()
		case <-n.closing:
			after.Stop()
			return nil
		}
		if err := n.emit(now); err != nil {
			return err
		}
	}
	ticker := time.NewTicker(n.s.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-n.closing:
			return nil
		case now := <-ticker.C:
			if err := n.emit(now); err != nil {
				return err
			}
		}
	}
}

// Emit a set of stats data points.
func (n *StatsNode) emit(now time.Time) error {
	n.timer.Start()
	defer n.timer.Stop()

	name := "stats"
	t := now.UTC()
	if n.s.AlignFlag {
		t = t.Round(n.s.Interval)
	}
	stats := n.en.nodeStatsByGroup()
	for _, stat := range stats {
		point := edge.NewPointMessage(
			name, "", "",
			stat.Dimensions,
			stat.Fields,
			stat.Tags,
			t,
		)
		n.timer.Pause()
		for _, out := range n.outs {
			err := out.Collect(point)
			if err != nil {
				return err
			}
		}
		n.timer.Resume()
	}
	return nil
}

func (n *StatsNode) stopStats() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.closed {
		n.closed = true
		close(n.closing)
	}
}
