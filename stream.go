package kapacitor

import (
	"fmt"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/tick"
)

type StreamNode struct {
	node
	s          *pipeline.StreamNode
	expression *tick.StatefulExpr
	db         string
	rp         string
	name       string
}

// Create a new  StreamNode which filters data from a source.
func newStreamNode(et *ExecutingTask, n *pipeline.StreamNode) (*StreamNode, error) {
	sn := &StreamNode{
		node: node{Node: n, et: et},
		s:    n,
	}
	sn.node.runF = sn.runStream
	var err error
	if sn.s.From != "" {
		sn.db, sn.rp, sn.name, err = parseFromClause(sn.s.From)
		if err != nil {
			return nil, fmt.Errorf("error parsing FROM clause %q %v", sn.s.From, err)
		}
	}
	if n.Expression != nil {
		sn.expression = tick.NewStatefulExpr(n.Expression)
	}
	return sn, nil
}

func parseFromClause(from string) (db, rp, mm string, err error) {
	//create fake but complete query for parsing
	query := "select v from " + from
	s, err := influxql.ParseStatement(query)
	if err != nil {
		return "", "", "", err
	}
	if slct, ok := s.(*influxql.SelectStatement); ok && len(slct.Sources) == 1 {
		if m, ok := slct.Sources[0].(*influxql.Measurement); ok {
			return m.Database, m.RetentionPolicy, m.Name, nil
		}
	}
	return "", "", "", fmt.Errorf("invalid from condition: %q", from)
}

func (s *StreamNode) runStream() error {

	for pt, ok := s.ins[0].NextPoint(); ok; pt, ok = s.ins[0].NextPoint() {
		if s.matches(pt) {
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
