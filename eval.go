package kapacitor

import (
	"errors"
	"log"
	"time"

	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick"
)

const (
	statsEvalErrors = "eval_errors"
)

type EvalNode struct {
	node
	e           *pipeline.EvalNode
	expressions []*tick.StatefulExpr
	evalErrors  *expvar.Int
}

// Create a new  EvalNode which applies a transformation func to each point in a stream and returns a single point.
func newEvalNode(et *ExecutingTask, n *pipeline.EvalNode, l *log.Logger) (*EvalNode, error) {
	if len(n.AsList) != len(n.Expressions) {
		return nil, errors.New("must provide one name per expression via the 'As' property")
	}
	en := &EvalNode{
		node: node{Node: n, et: et, logger: l},
		e:    n,
	}
	// Create stateful expressions
	en.expressions = make([]*tick.StatefulExpr, len(n.Expressions))
	for i, expr := range n.Expressions {
		en.expressions[i] = tick.NewStatefulExpr(expr)
	}

	en.node.runF = en.runEval
	return en, nil
}

func (e *EvalNode) runEval(snapshot []byte) error {
	e.evalErrors = &expvar.Int{}
	e.statMap.Set(statsEvalErrors, e.evalErrors)
	switch e.Provides() {
	case pipeline.StreamEdge:
		for p, ok := e.ins[0].NextPoint(); ok; p, ok = e.ins[0].NextPoint() {
			e.timer.Start()
			p.Fields = e.eval(p.Time, p.Fields, p.Tags)
			e.timer.Stop()
			for _, child := range e.outs {
				err := child.CollectPoint(p)
				if err != nil {
					return err
				}
			}
		}
	case pipeline.BatchEdge:
		for b, ok := e.ins[0].NextBatch(); ok; b, ok = e.ins[0].NextBatch() {
			e.timer.Start()
			for i, p := range b.Points {
				b.Points[i].Fields = e.eval(p.Time, p.Fields, p.Tags)
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

func (e *EvalNode) eval(now time.Time, fields models.Fields, tags map[string]string) models.Fields {
	vars, err := mergeFieldsAndTags(now, fields, tags)
	if err != nil {
		e.logger.Println("E!", err)
		return nil
	}
	for i, expr := range e.expressions {
		if v, err := expr.EvalNum(vars); err == nil {
			name := e.e.AsList[i]
			vars.Set(name, v)
		} else {
			e.evalErrors.Add(1)
			if !e.e.QuiteFlag {
				e.logger.Println("E!", err)
			}
		}
	}
	var newFields models.Fields
	if e.e.KeepFlag {
		if l := len(e.e.KeepList); l != 0 {
			newFields = make(models.Fields, l)
			for _, f := range e.e.KeepList {
				if v, err := vars.Get(f); err == nil {
					newFields[f] = v
				} else {
					e.evalErrors.Add(1)
					if !e.e.QuiteFlag {
						e.logger.Println("E!", err)
					}
				}
			}
		} else {
			newFields = make(models.Fields, len(fields)+len(e.e.AsList))
			for f, v := range fields {
				newFields[f] = v
			}
			for _, f := range e.e.AsList {
				if v, err := vars.Get(f); err == nil {
					newFields[f] = v
				} else {
					e.evalErrors.Add(1)
					if !e.e.QuiteFlag {
						e.logger.Println("E!", err)
					}
				}
			}
		}
	} else {
		newFields = make(models.Fields, len(e.e.AsList))
		for _, f := range e.e.AsList {
			if v, err := vars.Get(f); err == nil {
				newFields[f] = v
			} else {
				e.evalErrors.Add(1)
				if !e.e.QuiteFlag {
					e.logger.Println("E!", err)
				}
			}
		}
	}
	return newFields
}
