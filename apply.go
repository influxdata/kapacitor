package kapacitor

import (
	"fmt"

	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

type TransFunc func(p *models.Point) (*models.Point, error)

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
		for p := a.ins[0].NextPoint(); p != nil; p = a.ins[0].NextPoint() {
			np, err := a.Func(p)
			if err != nil {
				return err
			}
			for _, child := range a.outs {
				err := child.CollectPoint(np)
				if err != nil {
					return err
				}
			}
		}
	case pipeline.BatchEdge:
		for b := a.ins[0].NextBatch(); b != nil; b = a.ins[0].NextBatch() {
			nb := make([]*models.Point, len(b))
			for i, p := range b {
				np, err := a.Func(p)
				if err != nil {
					return err
				}
				nb[i] = np
			}
			for _, child := range a.outs {
				err := child.CollectBatch(nb)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
