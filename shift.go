package kapacitor

import (
	"errors"
	"log"
	"time"

	"github.com/influxdata/kapacitor/pipeline"
)

type ShiftNode struct {
	node
	s *pipeline.ShiftNode

	shift time.Duration
}

// Create a new  ShiftNode which shifts points and batches in time.
func newShiftNode(et *ExecutingTask, n *pipeline.ShiftNode, l *log.Logger) (*ShiftNode, error) {
	sn := &ShiftNode{
		node:  node{Node: n, et: et, logger: l},
		s:     n,
		shift: n.Shift,
	}
	sn.node.runF = sn.runShift
	if n.Shift == 0 {
		return nil, errors.New("invalid shift value: must be non zero duration")
	}
	return sn, nil
}

func (s *ShiftNode) runShift([]byte) error {
	switch s.Wants() {
	case pipeline.StreamEdge:
		for p, ok := s.ins[0].NextPoint(); ok; p, ok = s.ins[0].NextPoint() {
			s.timer.Start()
			p.Time = p.Time.Add(s.shift)
			s.timer.Stop()
			for _, child := range s.outs {
				err := child.CollectPoint(p)
				if err != nil {
					return err
				}
			}
		}
	case pipeline.BatchEdge:
		for b, ok := s.ins[0].NextBatch(); ok; b, ok = s.ins[0].NextBatch() {
			s.timer.Start()
			b.TMax = b.TMax.Add(s.shift)
			for i, p := range b.Points {
				b.Points[i].Time = p.Time.Add(s.shift)
			}
			s.timer.Stop()
			for _, child := range s.outs {
				err := child.CollectBatch(b)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
