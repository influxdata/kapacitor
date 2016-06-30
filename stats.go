package kapacitor

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/models"
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
func newStatsNode(et *ExecutingTask, n *pipeline.StatsNode, l *log.Logger) (*StatsNode, error) {
	// Lookup the executing node for stats.
	en := et.lookup[n.SourceNode.ID()]
	if en == nil {
		return nil, fmt.Errorf("no node found for %s", n.SourceNode.Name())
	}
	sn := &StatsNode{
		node:    node{Node: n, et: et, logger: l},
		s:       n,
		en:      en,
		closing: make(chan struct{}),
	}
	sn.node.runF = sn.runStats
	sn.node.stopF = sn.stopStats
	return sn, nil
}

func (s *StatsNode) runStats([]byte) error {
	if s.s.AlignFlag {
		// Wait till we are roughly aligned with the interval.
		now := time.Now()
		next := now.Truncate(s.s.Interval).Add(s.s.Interval)
		after := time.NewTicker(next.Sub(now))
		select {
		case <-after.C:
			after.Stop()
		case <-s.closing:
			after.Stop()
			return nil
		}
		if err := s.emit(now); err != nil {
			return err
		}
	}
	ticker := time.NewTicker(s.s.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.closing:
			return nil
		case now := <-ticker.C:
			if err := s.emit(now); err != nil {
				return err
			}
		}
	}
}

// Emit a set of stats data points.
func (s *StatsNode) emit(now time.Time) error {
	s.timer.Start()
	point := models.Point{
		Name: "stats",
		Tags: map[string]string{"node": s.en.Name()},
		Time: now.UTC(),
	}
	if s.s.AlignFlag {
		point.Time = point.Time.Round(s.s.Interval)
	}
	stats := s.en.nodeStatsByGroup()
	for group, stat := range stats {
		point.Fields = stat.Fields
		point.Group = group
		point.Dimensions = stat.Dimensions
		point.Tags = stat.Tags
		s.timer.Pause()
		for _, out := range s.outs {
			err := out.CollectPoint(point)
			if err != nil {
				return err
			}
		}
		s.timer.Resume()
	}
	s.timer.Stop()
	return nil
}

func (s *StatsNode) stopStats() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.closing)
	}
}
