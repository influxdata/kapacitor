package kapacitor

import (
	"errors"

	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/tick"
)

type WhereNode struct {
	node
	w           *pipeline.WhereNode
	endpoint    string
	expressions map[models.GroupID]*tick.StatefulExpr
}

// Create a new WhereNode which filters down the batch or stream by a condition
func newWhereNode(et *ExecutingTask, n *pipeline.WhereNode) (wn *WhereNode, err error) {
	wn = &WhereNode{
		node:        node{Node: n, et: et},
		w:           n,
		expressions: make(map[models.GroupID]*tick.StatefulExpr),
	}
	wn.runF = wn.runWhere
	if n.Expression == nil {
		return nil, errors.New("nil expression passed to WhereNode")
	}
	return
}

func (w *WhereNode) runWhere() error {
	switch w.Wants() {
	case pipeline.StreamEdge:
		for p, ok := w.ins[0].NextPoint(); ok; p, ok = w.ins[0].NextPoint() {
			expr := w.expressions[p.Group]
			if expr == nil {
				expr = tick.NewStatefulExpr(w.w.Expression)
				w.expressions[p.Group] = expr
			}
			if pass, err := EvalPredicate(expr, p.Fields, p.Tags); pass {
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
			expr := w.expressions[b.Group]
			if expr == nil {
				expr = tick.NewStatefulExpr(w.w.Expression)
				w.expressions[b.Group] = expr
			}
			for i, p := range b.Points {
				if pass, err := EvalPredicate(expr, p.Fields, p.Tags); !pass {
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
