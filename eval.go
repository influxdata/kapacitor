package kapacitor

import (
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/tick"
)

type EvalNode struct {
	node
	e          *pipeline.EvalNode
	expression *tick.StatefulExpr
}

// Create a new  ApplyNode which applies a transformation func to each point in a stream and returns a single point.
func newApplyNode(et *ExecutingTask, n *pipeline.EvalNode) (*EvalNode, error) {
	en := &EvalNode{
		node:       node{Node: n, et: et},
		e:          n,
		expression: tick.NewStatefulExpr(n.Expression),
	}
	en.node.runF = en.runApply
	return en, nil
}

func (e *EvalNode) runApply() error {
	switch e.Provides() {
	case pipeline.StreamEdge:
		for p, ok := e.ins[0].NextPoint(); ok; p, ok = e.ins[0].NextPoint() {
			fields, err := e.eval(p.Fields, p.Tags)
			if err != nil {
				return err
			}
			p.Fields = fields
			for _, child := range e.outs {
				err := child.CollectPoint(p)
				if err != nil {
					return err
				}
			}
		}
	case pipeline.BatchEdge:
		for b, ok := e.ins[0].NextBatch(); ok; b, ok = e.ins[0].NextBatch() {
			for i, p := range b.Points {
				fields, err := e.eval(p.Fields, b.Tags)
				if err != nil {
					return err
				}
				b.Points[i].Fields = fields
			}
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

func (e *EvalNode) eval(fields models.Fields, tags map[string]string) (models.Fields, error) {
	vars, err := mergeFieldsAndTags(fields, tags)
	if err != nil {
		return nil, err
	}
	v, err := e.expression.EvalNum(vars)
	if err != nil {
		return nil, err
	}
	fields[e.e.As] = v
	return fields, nil
}
