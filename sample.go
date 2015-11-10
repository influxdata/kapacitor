package kapacitor

import (
	"errors"
	"time"

	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

type SampleNode struct {
	node
	s *pipeline.SampleNode

	counts   map[models.GroupID]int64
	duration time.Duration
}

// Create a new  SampleNode which filters data from a source.
func newSampleNode(et *ExecutingTask, n *pipeline.SampleNode) (*SampleNode, error) {
	sn := &SampleNode{
		node:     node{Node: n, et: et},
		s:        n,
		counts:   make(map[models.GroupID]int64),
		duration: n.Duration,
	}
	sn.node.runF = sn.runSample
	if n.Duration == 0 && n.Count == 0 {
		return nil, errors.New("invalid sample rate: must be positive integer or duration")
	}
	return sn, nil
}

func (s *SampleNode) runSample() error {
	switch s.Wants() {
	case pipeline.StreamEdge:
		for p, ok := s.ins[0].NextPoint(); ok; p, ok = s.ins[0].NextPoint() {
			if s.shouldKeep(p.Group, p.Time) {
				for _, child := range s.outs {
					err := child.CollectPoint(p)
					if err != nil {
						return err
					}
				}
			}
		}
	case pipeline.BatchEdge:
		for b, ok := s.ins[0].NextBatch(); ok; b, ok = s.ins[0].NextBatch() {
			if s.shouldKeep(b.Group, b.TMax) {
				for _, child := range s.outs {
					err := child.CollectBatch(b)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (s *SampleNode) shouldKeep(group models.GroupID, t time.Time) bool {
	if s.duration != 0 {
		keepTime := t.Truncate(s.duration)
		return t.Equal(keepTime)
	} else {
		count := s.counts[group]
		keep := count%s.s.Count == 0
		count++
		s.counts[group] = count
		return keep
	}
}
