package kapacitor

import (
	"errors"
	"fmt"
	"math"

	"github.com/influxdb/kapacitor/expr"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

type WhereNode struct {
	node
	w         *pipeline.WhereNode
	endpoint  string
	predicate *expr.Tree
	funcs     expr.Funcs
	fs        []exprFunc
}

type exprFunc interface {
	name() string
	fnc() expr.Func
}

// Create a new  WhereNode which filters down the batch or stream by a condition
func newWhereNode(et *ExecutingTask, n *pipeline.WhereNode) (wn *WhereNode, err error) {
	wn = &WhereNode{
		node: node{Node: n, et: et},
		w:    n,
	}
	wn.runF = wn.runWhere
	// Parse predicate
	wn.predicate, err = expr.Parse(n.Predicate)
	if err != nil {
		return nil, err
	}
	if wn.predicate.RType() != expr.ReturnBool {
		return nil, fmt.Errorf("Predicate does not evaluate to boolean value %q", n.Predicate)
	}

	// Initialize functions for the predicate
	wn.fs = append(wn.fs, &sigma{})

	wn.funcs = make(expr.Funcs)
	for _, f := range wn.fs {
		wn.funcs[f.name()] = f.fnc()
	}
	return
}

func (w *WhereNode) runWhere() error {
	switch w.Wants() {
	case pipeline.StreamEdge:
		for p, ok := w.ins[0].NextPoint(); ok; p, ok = w.ins[0].NextPoint() {
			if w.check(p.Fields) {
				for _, child := range w.outs {
					err := child.CollectPoint(p)
					if err != nil {
						return err
					}
				}
			}
		}
	case pipeline.BatchEdge:
		for b, ok := w.ins[0].NextBatch(); ok; b, ok = w.ins[0].NextBatch() {
			for i, p := range b.Points {
				if !w.check(p.Fields) {
					b.Points = append(b.Points[:i], b.Points[i+1:]...)
				}
			}
			for _, child := range w.outs {
				err := child.CollectBatch(b)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// check if a set of fields and values match the condition
func (w *WhereNode) check(fields models.Fields) bool {
	vars := make(expr.Vars)
	for k, v := range fields {
		if f, ok := v.(float64); ok {
			vars[k] = f
		} else {
			w.logger.Println("W! fields values must be float64")
			return false
		}
	}
	b, err := w.predicate.EvalBool(vars, w.funcs)
	if err != nil {
		w.logger.Println("E! error evaluating WHERE:", err)
		return false
	}
	return b
}

type sigma struct {
	mean     float64
	variance float64
	m2       float64
	n        float64
}

func (s *sigma) name() string {
	return "sigma"
}

func (s *sigma) fnc() expr.Func {
	return s.call
}

func (s *sigma) call(args ...float64) (float64, error) {
	if len(args) != 1 {
		return 0, errors.New("sigma expected exactly one argument")
	}
	x := args[0]
	s.n++
	delta := x - s.mean
	s.mean = s.mean + delta/s.n
	s.m2 = s.m2 + delta*(x-s.mean)
	s.variance = s.m2 / (s.n - 1)

	if s.n < 2 {
		return 0, nil
	}
	return math.Abs(x-s.mean) / math.Sqrt(s.variance), nil

}
