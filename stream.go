package kapacitor

import (
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/tick"
)

type StreamNode struct {
	node
	s             *pipeline.StreamNode
	expression    *tick.StatefulExpr
	dimensions    []string
	allDimensions bool
	db            string
	rp            string
	name          string
}

// Create a new  StreamNode which filters data from a source.
func newStreamNode(et *ExecutingTask, n *pipeline.StreamNode) (*StreamNode, error) {
	sn := &StreamNode{
		node: node{Node: n, et: et},
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

func (s *StreamNode) runStream() error {

	for pt, ok := s.ins[0].NextPoint(); ok; pt, ok = s.ins[0].NextPoint() {
		if s.matches(pt) {
			if s.s.Truncate != 0 {
				pt.Time = pt.Time.Truncate(s.s.Truncate)
			}
			pt = setGroupOnPoint(pt, s.allDimensions, s.dimensions)
			for _, child := range s.outs {
				err := child.CollectPoint(pt)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *StreamNode) matches(p models.Point) bool {
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
		if pass, err := EvalPredicate(s.expression, p.Fields, p.Tags); err != nil {
			s.logger.Println("E! error while evaluating WHERE expression:", err)
			return false
		} else {
			return pass
		}
	}
	return true
}
