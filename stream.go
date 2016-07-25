package kapacitor

import (
	"fmt"
	"log"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/stateful"
)

type StreamNode struct {
	node
	s *pipeline.StreamNode
}

// Create a new  StreamNode which copies all data to children
func newStreamNode(et *ExecutingTask, n *pipeline.StreamNode, l *log.Logger) (*StreamNode, error) {
	sn := &StreamNode{
		node: node{Node: n, et: et, logger: l},
		s:    n,
	}
	sn.node.runF = sn.runSourceStream
	return sn, nil
}

func (s *StreamNode) runSourceStream([]byte) error {
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
	expression    stateful.Expression
	scopePool     stateful.ScopePool
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

	if n.Lambda != nil {
		expr, err := stateful.NewExpression(n.Lambda.Expression)
		if err != nil {
			return nil, fmt.Errorf("Failed to compile from expression: %v", err)
		}

		sn.expression = expr
		sn.scopePool = stateful.NewScopePool(stateful.FindReferenceVariables(n.Lambda.Expression))
	}

	return sn, nil
}

func (s *FromNode) runStream([]byte) error {
	dims := models.Dimensions{
		ByName: s.s.GroupByMeasurementFlag,
	}
	for pt, ok := s.ins[0].NextPoint(); ok; pt, ok = s.ins[0].NextPoint() {
		s.timer.Start()
		if s.matches(pt) {
			if s.s.Truncate != 0 {
				pt.Time = pt.Time.Truncate(s.s.Truncate)
			}
			if s.s.Round != 0 {
				pt.Time = pt.Time.Round(s.s.Round)
			}
			dims.TagNames = s.dimensions
			pt = setGroupOnPoint(pt, s.allDimensions, dims)
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
		if pass, err := EvalPredicate(s.expression, s.scopePool, p.Time, p.Fields, p.Tags); err != nil {
			s.logger.Println("E! error while evaluating WHERE expression:", err)
			return false
		} else {
			return pass
		}
	}
	return true
}
