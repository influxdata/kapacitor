package kapacitor

import (
	"fmt"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

type StreamNode struct {
	node
	s         *pipeline.StreamNode
	condition influxql.Expr
	db        string
	rp        string
	name      string
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
	if sn.s.Predicate != "" {
		//Parse where condition
		sn.condition, err = parseWhereCondition(sn.s.Predicate)
		if err != nil {
			return nil, fmt.Errorf("error parsing WHERE clause  %q %v", sn.s.Predicate, err)
		}
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

func parseWhereCondition(where string) (influxql.Expr, error) {
	//create fake but complete query for parsing
	query := "select v from m where " + where
	s, err := influxql.ParseStatement(query)
	if err != nil {
		return nil, err
	}
	if slct, ok := s.(*influxql.SelectStatement); ok {
		return slct.Condition, nil
	}
	return nil, fmt.Errorf("invalid where condition: %q", where)
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
	if !s.evalExpr(p, s.condition) {
		return false
	}
	return true
}

//evaluate a given influxql.Expr a against a Point
func (s *StreamNode) evalExpr(p models.Point, expr influxql.Expr) bool {
	if expr == nil {
		return true
	}
	switch expr.(type) {
	case *influxql.BinaryExpr:
		be := expr.(*influxql.BinaryExpr)
		var key string
		var value string
		switch be.LHS.(type) {
		case *influxql.VarRef:
			lit, ok := be.RHS.(*influxql.StringLiteral)
			if !ok {
				s.logger.Println("E! unexpected RHS expected StringLiteral", be.RHS)
				return false
			}
			key = be.LHS.(*influxql.VarRef).Val
			value = lit.Val
		case *influxql.StringLiteral:
			ref, ok := be.RHS.(*influxql.VarRef)
			if !ok {
				s.logger.Println("E! unexpected RHS expected VarRef", be.RHS)
				return false
			}
			key = ref.Val
			value = be.LHS.(*influxql.StringLiteral).Val
		}
		switch be.Op {
		case influxql.EQ:
			return p.Tags[key] == value
		case influxql.NEQ:
			return p.Tags[key] != value
		}
	default:
		s.logger.Println("E! unexpected expr", expr)
		return false

	}
	return true
}
