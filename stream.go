package kapacitor

import (
	"fmt"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/kapacitor/expr"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

type StreamNode struct {
	node
	s         *pipeline.StreamNode
	predicate *expr.Tree
	db        string
	rp        string
	name      string
	funcs     expr.Funcs
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
	// Parse predicate
	if n.Predicate != "" {
		sn.predicate, err = expr.Parse(n.Predicate)
		if err != nil {
			return nil, err
		}
		if sn.predicate.RType() != expr.ReturnBool {
			return nil, fmt.Errorf("WHERE clause does not evaluate to boolean value %q", n.Predicate)
		}
	}
	// Initialize expr functions
	sn.funcs = expr.Functions()
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
	if !s.evalPredicate(p) {
		return false
	}
	return true
}

//evaluate a given predicate a against a Point
func (s *StreamNode) evalPredicate(p models.Point) bool {
	if s.predicate == nil {
		return true
	}
	vars := make(expr.Vars)
	for k, v := range p.Fields {
		if p.Tags[k] != "" {
			s.logger.Println("E! cannot have field and tags with same name")
			return false
		}
		vars[k] = v
	}
	for k, v := range p.Tags {
		vars[k] = v
	}

	b, err := s.predicate.EvalBool(vars, s.funcs)
	if err != nil {
		s.logger.Println("E! error evaluating WHERE:", err)
		return false
	}
	return b
}
