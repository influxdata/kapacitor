package kapacitor

import (
	"errors"
	"fmt"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

type StreamNode struct {
	node
	s *pipeline.StreamNode
}

// Create a new  StreamNode which copies all data to children
func newStreamNode(et *ExecutingTask, n *pipeline.StreamNode, d NodeDiagnostic) (*StreamNode, error) {
	sn := &StreamNode{
		node: node{Node: n, et: et, diag: d},
		s:    n,
	}
	sn.node.runF = sn.runSourceStream
	return sn, nil
}

func (n *StreamNode) runSourceStream([]byte) error {
	for m, ok := n.ins[0].Emit(); ok; m, ok = n.ins[0].Emit() {
		for _, child := range n.outs {
			err := child.Collect(m)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type FromNode struct {
	node
	s             *pipeline.FromNode
	expression    stateful.Expression
	scopePool     stateful.ScopePool
	tagNames      []string
	allDimensions bool
	db            string
	rp            string
	name          string
}

// Create a new  FromNode which filters data from a source.
func newFromNode(et *ExecutingTask, n *pipeline.FromNode, d NodeDiagnostic) (*FromNode, error) {
	sn := &FromNode{
		node: node{Node: n, et: et, diag: d},
		s:    n,
		db:   n.Database,
		rp:   n.RetentionPolicy,
		name: n.Measurement,
	}
	sn.node.runF = sn.runStream
	sn.allDimensions, sn.tagNames = determineTagNames(n.Dimensions, nil)

	if n.Lambda != nil {
		expr, err := stateful.NewExpression(n.Lambda.Expression)
		if err != nil {
			return nil, fmt.Errorf("Failed to compile from expression: %v", err)
		}

		sn.expression = expr
		sn.scopePool = stateful.NewScopePool(ast.FindReferenceVariables(n.Lambda.Expression))
	}

	return sn, nil
}

func (n *FromNode) runStream([]byte) error {
	consumer := edge.NewConsumerWithReceiver(
		n.ins[0],
		edge.NewReceiverFromForwardReceiverWithStats(
			n.outs,
			edge.NewTimedForwardReceiver(n.timer, n),
		),
	)
	return consumer.Consume()
}
func (n *FromNode) BeginBatch(edge.BeginBatchMessage) (edge.Message, error) {
	return nil, errors.New("from does not support batch data")
}
func (n *FromNode) BatchPoint(edge.BatchPointMessage) (edge.Message, error) {
	return nil, errors.New("from does not support batch data")
}
func (n *FromNode) EndBatch(edge.EndBatchMessage) (edge.Message, error) {
	return nil, errors.New("from does not support batch data")
}

func (n *FromNode) Point(p edge.PointMessage) (edge.Message, error) {
	if n.matches(p) {
		p = p.ShallowCopy()
		if n.s.Truncate != 0 {
			p.SetTime(p.Time().Truncate(n.s.Truncate))
		}
		if n.s.Round != 0 {
			p.SetTime(p.Time().Round(n.s.Round))
		}
		p.SetDimensions(models.Dimensions{
			ByName:   n.s.GroupByMeasurementFlag,
			TagNames: computeTagNames(p.Tags(), n.allDimensions, n.tagNames, nil),
		})
		return p, nil
	}
	return nil, nil
}

func (n *FromNode) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (n *FromNode) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (n *FromNode) Done() {}

func (n *FromNode) matches(p edge.PointMessage) bool {
	if n.db != "" && p.Database() != n.db {
		return false
	}
	if n.rp != "" && p.RetentionPolicy() != n.rp {
		return false
	}
	if n.name != "" && p.Name() != n.name {
		return false
	}
	if n.expression != nil {
		if pass, err := EvalPredicate(n.expression, n.scopePool, p); err != nil {
			n.diag.Error("failed to evaluate WHERE expression", err)
			return false
		} else {
			return pass
		}
	}
	return true
}
