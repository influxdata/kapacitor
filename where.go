package kapacitor

import (
	"github.com/influxdb/kapacitor/expr"
	"github.com/influxdb/kapacitor/pipeline"
)

type WhereNode struct {
	node
	w         *pipeline.WhereNode
	endpoint  string
	predicate *expr.StatefulExpr
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
	tree, err := expr.ParseForType(n.Predicate, expr.ReturnBool)
	if err != nil {
		return nil, err
	}
	wn.predicate = &expr.StatefulExpr{tree, expr.Functions()}
	return
}

func (w *WhereNode) runWhere() error {
	switch w.Wants() {
	case pipeline.StreamEdge:
		for p, ok := w.ins[0].NextPoint(); ok; p, ok = w.ins[0].NextPoint() {
			if pass, err := EvalPredicate(w.predicate, p.Fields, p.Tags); pass {
				for _, child := range w.outs {
					err := child.CollectPoint(p)
					if err != nil {
						return err
					}
				}
			} else if err != nil {
				w.logger.Println("E! error while evaluating expression:", err)
			}
		}
	case pipeline.BatchEdge:
		for b, ok := w.ins[0].NextBatch(); ok; b, ok = w.ins[0].NextBatch() {
			for i, p := range b.Points {
				if pass, err := EvalPredicate(w.predicate, p.Fields, b.Tags); !pass {
					if err != nil {
						w.logger.Println("E! error while evaluating WHERE expression:", err)
					}
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
