package kapacitor

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

type EvalNode struct {
	node
	e                  *pipeline.EvalNode
	expressions        []stateful.Expression
	expressionsByGroup map[models.GroupID][]stateful.Expression
	refVarList         [][]string
	scopePool          stateful.ScopePool
	tags               map[string]bool

	expressionsByGroupMu sync.RWMutex

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
	en.refVarList = make([][]string, len(n.Lambdas))
	expressions := make([]ast.Node, len(n.Lambdas))
	for i, lambda := range n.Lambdas {
		expressions[i] = lambda.Expression
		statefulExpr, err := stateful.NewExpression(lambda.Expression)
		if err != nil {
			return nil, fmt.Errorf("Failed to compile %v expression: %v", i, err)
		}
		en.expressions[i] = statefulExpr
		refVars := ast.FindReferenceVariables(lambda.Expression)
		en.refVarList[i] = refVars
	}
	// Create a single pool for the combination of all expressions
	en.scopePool = stateful.NewScopePool(ast.FindReferenceVariables(expressions...))

	// Create map of tags
	if l := len(n.TagsList); l > 0 {
		en.tags = make(map[string]bool, l)
		for _, tag := range n.TagsList {
			en.tags[tag] = true
		}
	}

	en.node.runF = en.runEval
	return en, nil
}

func (e *EvalNode) runEval(snapshot []byte) error {
	valueF := func() int64 {
		e.expressionsByGroupMu.RLock()
		l := len(e.expressionsByGroup)
		e.expressionsByGroupMu.RUnlock()
		return int64(l)
	}
	e.statMap.Set(statCardinalityGauge, expvar.NewIntFuncGauge(valueF))

	switch e.Provides() {
	case pipeline.StreamEdge:
		var err error
		for p, ok := e.ins[0].NextPoint(); ok; p, ok = e.ins[0].NextPoint() {
			e.timer.Start()
			p.Fields, p.Tags, err = e.eval(p.Time, p.Group, p.Fields, p.Tags)
			if err != nil {
				e.incrementErrorCount()
				if !e.e.QuietFlag {
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
			b.Points = b.ShallowCopyPoints()
			for i := 0; i < len(b.Points); {
				p := b.Points[i]
				b.Points[i].Fields, b.Points[i].Tags, err = e.eval(p.Time, b.Group, p.Fields, p.Tags)
				if err != nil {
					e.incrementErrorCount()
					if !e.e.QuietFlag {
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

func (e *EvalNode) eval(now time.Time, group models.GroupID, fields models.Fields, tags models.Tags) (models.Fields, models.Tags, error) {
	vars := e.scopePool.Get()
	defer e.scopePool.Put(vars)
	e.expressionsByGroupMu.RLock()
	expressions, ok := e.expressionsByGroup[group]
	e.expressionsByGroupMu.RUnlock()
	if !ok {
		expressions = make([]stateful.Expression, len(e.expressions))
		for i, exp := range e.expressions {
			expressions[i] = exp.CopyReset()
		}
		e.expressionsByGroupMu.Lock()
		e.expressionsByGroup[group] = expressions
		e.expressionsByGroupMu.Unlock()
	}
	for i, expr := range expressions {
		err := fillScope(vars, e.refVarList[i], now, fields, tags)
		if err != nil {
			return nil, nil, err
		}
		v, err := expr.Eval(vars)
		if err != nil {
			return nil, nil, err
		}
		name := e.e.AsList[i]
		vars.Set(name, v)
	}
	newTags := tags
	if len(e.tags) > 0 {
		newTags = newTags.Copy()
		for tag := range e.tags {
			v, err := vars.Get(tag)
			if err != nil {
				return nil, nil, err
			}
			if s, ok := v.(string); !ok {
				return nil, nil, fmt.Errorf("result of a tag expression must be of type string, got %T", v)
			} else {
				newTags[tag] = s
			}
		}
	}
	var newFields models.Fields
	if e.e.KeepFlag {
		if l := len(e.e.KeepList); l != 0 {
			newFields = make(models.Fields, l)
			for _, f := range e.e.KeepList {
				// Try the vars scope first
				if vars.Has(f) {
					v, err := vars.Get(f)
					if err != nil {
						return nil, nil, err
					}
					newFields[f] = v
				} else if v, ok := fields[f]; ok {
					// Try the raw fields next, since it may not have been a referenced var.
					newFields[f] = v
				} else {
					return nil, nil, fmt.Errorf("cannot keep field %q, field does not exist", f)
				}
			}
		} else {
			newFields = make(models.Fields, len(fields)+len(e.e.AsList))
			for f, v := range fields {
				newFields[f] = v
			}
			for _, f := range e.e.AsList {
				v, err := vars.Get(f)
				if err != nil {
					return nil, nil, err
				}
				newFields[f] = v
			}
		}
	} else {
		newFields = make(models.Fields, len(e.e.AsList)-len(e.tags))
		for _, f := range e.e.AsList {
			if e.tags[f] {
				continue
			}
			v, err := vars.Get(f)
			if err != nil {
				return nil, nil, err
			}
			newFields[f] = v
		}
	}
	return newFields, newTags, nil
}
