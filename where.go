package kapacitor

import (
	"errors"
	"fmt"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/stateful"
	"go.uber.org/zap"
)

type WhereNode struct {
	node
	w        *pipeline.WhereNode
	endpoint string

	expressions map[models.GroupID]stateful.Expression
	scopePools  map[models.GroupID]stateful.ScopePool
}

// Create a new WhereNode which filters down the batch or stream by a condition
func newWhereNode(et *ExecutingTask, n *pipeline.WhereNode, l zap.Logger) (wn *WhereNode, err error) {
	wn = &WhereNode{
		node:        node{Node: n, et: et, logger: l},
		w:           n,
		expressions: make(map[models.GroupID]stateful.Expression),
		scopePools:  make(map[models.GroupID]stateful.ScopePool),
	}
	wn.runF = wn.runWhere
	if n.Lambda == nil {
		return nil, errors.New("nil expression passed to WhereNode")
	}
	return
}

func (w *WhereNode) runWhere(snapshot []byte) error {
	switch w.Wants() {
	case pipeline.StreamEdge:
		for p, ok := w.ins[0].NextPoint(); ok; p, ok = w.ins[0].NextPoint() {
			w.timer.Start()
			expr := w.expressions[p.Group]
			scopePool := w.scopePools[p.Group]

			if expr == nil {
				compiledExpr, err := stateful.NewExpression(w.w.Lambda.Expression)
				if err != nil {
					return fmt.Errorf("Failed to compile expression in where clause: %v", err)
				}

				expr = compiledExpr
				w.expressions[p.Group] = expr

				scopePool = stateful.NewScopePool(stateful.FindReferenceVariables(w.w.Lambda.Expression))
				w.scopePools[p.Group] = scopePool
			}
			if pass, err := EvalPredicate(expr, scopePool, p.Time, p.Fields, p.Tags); pass {
				w.timer.Pause()
				for _, child := range w.outs {
					err := child.CollectPoint(p)
					if err != nil {
						return err
					}
				}
				w.timer.Resume()
			} else if err != nil {
				w.logger.Error("error while evaluating expression", zap.Error(err))
			}
			w.timer.Stop()
		}
	case pipeline.BatchEdge:
		for b, ok := w.ins[0].NextBatch(); ok; b, ok = w.ins[0].NextBatch() {
			w.timer.Start()
			expr := w.expressions[b.Group]
			scopePool := w.scopePools[b.Group]

			if expr == nil {
				compiledExpr, err := stateful.NewExpression(w.w.Lambda.Expression)
				if err != nil {
					return fmt.Errorf("Failed to compile expression in where clause: %v", err)
				}

				expr = compiledExpr
				w.expressions[b.Group] = expr

				scopePool = stateful.NewScopePool(stateful.FindReferenceVariables(w.w.Lambda.Expression))
				w.scopePools[b.Group] = scopePool
			}
			points := b.Points
			b.Points = make([]models.BatchPoint, 0, len(b.Points))
			for _, p := range points {
				if pass, err := EvalPredicate(expr, scopePool, p.Time, p.Fields, p.Tags); pass {
					if err != nil {
						w.logger.Error("error while evaluating WHERE expression", zap.Error(err))
					}
					b.Points = append(b.Points, p)
				}
			}
			w.timer.Stop()
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
