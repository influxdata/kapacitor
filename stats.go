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

	expireCount int
	zeros       map[models.GroupID]int
}

// Create a new  FromNode which filters data from a source.
func newStatsNode(et *ExecutingTask, n *pipeline.StatsNode, l *log.Logger) (*StatsNode, error) {
	// Lookup the executing node for stats.
	en := et.lookup[n.SourceNode.ID()]
	if en == nil {
		return nil, fmt.Errorf("no node found for %s", n.SourceNode.Name())
	}
	sn := &StatsNode{
		node:        node{Node: n, et: et, logger: l},
		s:           n,
		en:          en,
		expireCount: int(n.ExpireCount),
		closing:     make(chan struct{}),
		zeros:       make(map[models.GroupID]int),
	}
	sn.node.runF = sn.runStats
	sn.node.stopF = sn.stopStats
	return sn, nil
}

func (s *StatsNode) runStats([]byte) error {
	ticker := time.NewTicker(s.s.Interval)
	defer ticker.Stop()
	point := models.Point{
		Name: "stats",
		Tags: map[string]string{"node": s.en.Name()},
	}
	expiredGroups := make([]models.GroupID, 0)
	for {
		select {
		case <-s.closing:
			return nil
		case now := <-ticker.C:
			s.timer.Start()
			point.Time = now.UTC()
			stats := s.en.nodeStatsByGroup()
			for group, stat := range stats {
				point.Fields = stat.Fields
				point.Group = group
				point.Dimensions = stat.Dimensions
				point.Tags = stat.Tags

				if stat.IsZero {
					s.zeros[group]++
					if s.zeros[group] == s.expireCount {
						expiredGroups = append(expiredGroups, group)
						delete(s.zeros, group)
					}
				}
				s.timer.Pause()
				for _, out := range s.outs {
					err := out.CollectPoint(point)
					if err != nil {
						return err
					}
				}
				s.timer.Resume()
			}
			if len(expiredGroups) > 0 {
				s.en.expireGroups(expiredGroups)
				expiredGroups = expiredGroups[0:0]
			}
			s.timer.Stop()
		}
	}
}

func (s *StatsNode) stopStats() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.closing)
	}
}
