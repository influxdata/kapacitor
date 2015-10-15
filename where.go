package kapacitor

import (
	"fmt"

	"github.com/influxdb/kapacitor/expr"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

type WhereNode struct {
	node
	w         *pipeline.WhereNode
	endpoint  string
	predicate *expr.Tree
	fs        []exprFunc
	funcs     expr.Funcs
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
		return nil, fmt.Errorf("WHERE clause does not evaluate to boolean value %q", n.Predicate)
	}
	wn.funcs = expr.Functions()
	return
}

func (w *WhereNode) runWhere() error {
	switch w.Wants() {
	case pipeline.StreamEdge:
		for p, ok := w.ins[0].NextPoint(); ok; p, ok = w.ins[0].NextPoint() {
			if w.check(p.Fields, p.Tags) {
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
				if !w.check(p.Fields, b.Tags) {
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
func (w *WhereNode) check(fields models.Fields, tags map[string]string) bool {
	vars := make(expr.Vars)
	for k, v := range fields {
		if tags[k] != "" {
			w.logger.Println("E! cannot have field and tags with same name")
			return false
		}
		vars[k] = v
	}
	for k, v := range tags {
		vars[k] = v
	}
	b, err := w.predicate.EvalBool(vars, w.funcs)
	if err != nil {
		w.logger.Println("E! error evaluating WHERE:", err)
		return false
	}
	return b
}
