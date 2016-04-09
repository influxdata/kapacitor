package kapacitor

import (
	"log"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick"
)

type SourceStreamNode struct {
	node
	s *pipeline.SourceStreamNode
}

// Create a new  SourceStreamNode which copies all data to children
func newSourceStreamNode(et *ExecutingTask, n *pipeline.SourceStreamNode, l *log.Logger) (*SourceStreamNode, error) {
	sn := &SourceStreamNode{
		node: node{Node: n, et: et, logger: l},
		s:    n,
	}
	sn.node.runF = sn.runSourceStream
	return sn, nil
}

func (s *SourceStreamNode) runSourceStream([]byte) error {
	for pt, ok := s.ins[0].NextPoint(); ok; pt, ok = s.ins[0].NextPoint() {
		for _, child := range s.outs {
			err := child.CollectPoint(pt)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type FromNode struct {
	node
	s             *pipeline.FromNode
	expression    *tick.StatefulExpr
	dimensions    []string
	allDimensions bool
	db            string
	rp            string
	name          string
}

// Create a new  FromNode which filters data from a source.
func newFromNode(et *ExecutingTask, n *pipeline.FromNode, l *log.Logger) (*FromNode, error) {
	sn := &FromNode{
		node: node{Node: n, et: et, logger: l},
		s:    n,
		db:   n.Database,
		rp:   n.RetentionPolicy,
		name: n.Measurement,
	}
	sn.node.runF = sn.runStream
	sn.allDimensions, sn.dimensions = determineDimensions(n.Dimensions)

	if n.Expression != nil {
		sn.expression = tick.NewStatefulExpr(n.Expression)
	}

	return sn, nil
}

func (s *FromNode) runStream([]byte) error {
	for pt, ok := s.ins[0].NextPoint(); ok; pt, ok = s.ins[0].NextPoint() {
		s.timer.Start()
		if s.matches(pt) {
			if s.s.Truncate != 0 {
				pt.Time = pt.Time.Truncate(s.s.Truncate)
			}
			pt = setGroupOnPoint(pt, s.allDimensions, s.dimensions)
			s.timer.Pause()
			for _, child := range s.outs {
				err := child.CollectPoint(pt)
				if err != nil {
					return err
				}
			}
			s.timer.Resume()
		}
		s.timer.Stop()
	}
	return nil
}

func (s *FromNode) matches(p models.Point) bool {
	if s.db != "" && p.Database != s.db {
		return false
	}
	if s.rp != "" && p.RetentionPolicy != s.rp {
		return false
	}
	if s.name != "" && p.Name != s.name {
		return false
	}
	if s.expression != nil {
		if pass, err := EvalPredicate(s.expression, p.Time, p.Fields, p.Tags); err != nil {
			s.logger.Println("E! error while evaluating WHERE expression:", err)
			return false
		} else {
			return pass
		}
	}
	return true
}
