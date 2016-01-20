package kapacitor

import (
	"fmt"
	"log"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

type StatsNode struct {
	node
	s       *pipeline.StatsNode
	en      Node
	closing chan struct{}
}

// Create a new  StreamNode which filters data from a source.
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
	ticker := time.NewTicker(s.s.Interval)
	defer ticker.Stop()
	point := models.Point{
		Name: "stats",
		Tags: map[string]string{"node": s.en.Name()},
	}
	for {
		select {
		case <-s.closing:
			return nil
		case now := <-ticker.C:
			point.Time = now
			count := s.en.collectedCount()
			point.Fields = models.Fields{"collected": count}
			for _, out := range s.outs {
				err := out.CollectPoint(point)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (s *StatsNode) stopStats() {
	close(s.closing)
}
