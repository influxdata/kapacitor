package kapacitor

import (
	"fmt"

	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

type TransFunc func(f models.Fields) (models.Fields, error)

type ApplyNode struct {
	node
	a    *pipeline.ApplyNode
	Func TransFunc
}

// Create a new  ApplyNode which applies a transformation func to each point in a stream and returns a single point.
func newApplyNode(et *ExecutingTask, n *pipeline.ApplyNode) (*ApplyNode, error) {
	f, ok := n.Func.(TransFunc)
	if !ok {
		return nil, fmt.Errorf("invalid func given to apply node, func type:%T", n.Func)
	}
	an := &ApplyNode{
		node: node{Node: n, et: et},
		a:    n,
		Func: f,
	}
	an.node.runF = an.runApply
	return an, nil
}

func (a *ApplyNode) runApply() error {
	switch a.Provides() {
	case pipeline.StreamEdge:
		for p, ok := a.ins[0].NextPoint(); ok; p, ok = a.ins[0].NextPoint() {
			fields, err := a.Func(p.Fields)
			if err != nil {
				return err
			}
			p.Fields = fields
			for _, child := range a.outs {
				err := child.CollectPoint(p)
				if err != nil {
					return err
				}
			}
		}
	case pipeline.BatchEdge:
		for b, ok := a.ins[0].NextBatch(); ok; b, ok = a.ins[0].NextBatch() {
			for i, p := range b.Points {
				fields, err := a.Func(p.Fields)
				if err != nil {
					return err
				}
				b.Points[i].Fields = fields
			}
			for _, child := range a.outs {
				err := child.CollectBatch(b)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
