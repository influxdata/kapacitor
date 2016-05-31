package kapacitor

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

const (
	statsEvalErrors = "eval_errors"
)

type EvalNode struct {
	node
	e                  *pipeline.EvalNode
	expressions        []stateful.Expression
	expressionsByGroup map[models.GroupID][]stateful.Expression
	scopePool          stateful.ScopePool

	evalErrors *expvar.Int
}

// Create a new  EvalNode which applies a transformation func to each point in a stream and returns a single point.
func newEvalNode(et *ExecutingTask, n *pipeline.EvalNode, l *log.Logger) (*EvalNode, error) {
	if len(n.AsList) != len(n.Lambdas) {
		return nil, errors.New("must provide one name per expression via the 'As' property")
	}
	en := &EvalNode{
		node:               node{Node: n, et: et, logger: l},
		e:                  n,
		expressionsByGroup: make(map[models.GroupID][]stateful.Expression),
	}
	// Create stateful expressions
	en.expressions = make([]stateful.Expression, len(n.Lambdas))
	expressions := make([]ast.Node, len(n.Lambdas))
	for i, lambda := range n.Lambdas {
		expressions[i] = lambda.Expression
		statefulExpr, err := stateful.NewExpression(lambda.Expression)
		if err != nil {
			return nil, fmt.Errorf("Failed to compile %v expression: %v", i, err)
		}
		en.expressions[i] = statefulExpr
	}

	en.scopePool = stateful.NewScopePool(stateful.FindReferenceVariables(expressions...))
	en.node.runF = en.runEval
	return en, nil
}

func (e *EvalNode) runEval(snapshot []byte) error {
	e.evalErrors = &expvar.Int{}
	e.statMap.Set(statsEvalErrors, e.evalErrors)
	switch e.Provides() {
	case pipeline.StreamEdge:
		var err error
		for p, ok := e.ins[0].NextPoint(); ok; p, ok = e.ins[0].NextPoint() {
			e.timer.Start()
			p.Fields, err = e.eval(p.Time, p.Group, p.Fields, p.Tags)
			if err != nil {
				e.evalErrors.Add(1)
				if !e.e.QuiteFlag {
					e.logger.Println("E!", err)
				}
				e.timer.Stop()
				// Skip bad point
				continue
			}
			e.timer.Stop()
			for _, child := range e.outs {
				err := child.CollectPoint(p)
				if err != nil {
					return err
				}
			}
		}
	case pipeline.BatchEdge:
		var err error
		for b, ok := e.ins[0].NextBatch(); ok; b, ok = e.ins[0].NextBatch() {
			e.timer.Start()
			for i := 0; i < len(b.Points); {
				p := b.Points[i]
				b.Points[i].Fields, err = e.eval(p.Time, b.Group, p.Fields, p.Tags)
				if err != nil {
					e.evalErrors.Add(1)
					if !e.e.QuiteFlag {
						e.logger.Println("E!", err)
					}
					// Remove bad point
					b.Points = append(b.Points[:i], b.Points[i+1:]...)
				} else {
					i++
				}
			}
			e.timer.Stop()
			for _, child := range e.outs {
				err := child.CollectBatch(b)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (e *EvalNode) eval(now time.Time, group models.GroupID, fields models.Fields, tags map[string]string) (models.Fields, error) {
	vars := e.scopePool.Get()
	defer e.scopePool.Put(vars)
	err := fillScope(vars, e.scopePool.ReferenceVariables(), now, fields, tags)
	if err != nil {
		return nil, err
	}
	expressions, ok := e.expressionsByGroup[group]
	if !ok {
		expressions = make([]stateful.Expression, len(e.expressions))
		for i, exp := range e.expressions {
			expressions[i] = exp.CopyReset()
		}
		e.expressionsByGroup[group] = expressions
	}
	for i, expr := range expressions {
		v, err := expr.Eval(vars)
		if err != nil {
			return nil, err
		}
		name := e.e.AsList[i]
		vars.Set(name, v)
	}
	var newFields models.Fields
	if e.e.KeepFlag {
		if l := len(e.e.KeepList); l != 0 {
			newFields = make(models.Fields, l)
			for _, f := range e.e.KeepList {
				v, err := vars.Get(f)
				if err != nil {
					return nil, err
				}
				newFields[f] = v
			}
		} else {
			newFields = make(models.Fields, len(fields)+len(e.e.AsList))
			for f, v := range fields {
				newFields[f] = v
			}
			for _, f := range e.e.AsList {
				v, err := vars.Get(f)
				if err != nil {
					return nil, err
				}
				newFields[f] = v
			}
		}
	} else {
		newFields = make(models.Fields, len(e.e.AsList))
		for _, f := range e.e.AsList {
			v, err := vars.Get(f)
			if err != nil {
				return nil, err
			}
			newFields[f] = v
		}
	}
	return newFields, nil
}
