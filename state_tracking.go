package kapacitor

import (
	"fmt"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

type stateTracker interface {
	track(t time.Time, inState bool) interface{}
	reset()
}

type stateTrackingGroup struct {
	n *StateTrackingNode
	stateful.Expression
	tracker stateTracker
}

type StateTrackingNode struct {
	node
	as string

	expr      stateful.Expression
	scopePool stateful.ScopePool

	newTracker func() stateTracker
}

func (n *StateTrackingNode) runStateTracking(_ []byte) error {
	consumer := edge.NewGroupedConsumer(
		n.ins[0],
		n,
	)
	n.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())
	return consumer.Consume()
}

func (n *StateTrackingNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	return edge.NewReceiverFromForwardReceiverWithStats(
		n.outs,
		edge.NewTimedForwardReceiver(n.timer, n.newGroup()),
	), nil
}

func (n *StateTrackingNode) newGroup() *stateTrackingGroup {
	// Create a new tracking group
	g := &stateTrackingGroup{
		n: n,
	}

	g.Expression = n.expr.CopyReset()

	g.tracker = n.newTracker()
	return g
}

func (g *stateTrackingGroup) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	g.tracker.reset()
	return begin, nil
}

func (g *stateTrackingGroup) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	bp = bp.ShallowCopy()
	err := g.track(bp)
	if err != nil {
		g.n.diag.Error("error while evaluating expression", err)
		return nil, nil
	}
	return bp, nil
}

func (g *stateTrackingGroup) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return end, nil
}

func (g *stateTrackingGroup) Point(p edge.PointMessage) (edge.Message, error) {
	p = p.ShallowCopy()
	err := g.track(p)
	if err != nil {
		g.n.diag.Error("error while evaluating expression", err)
		return nil, nil
	}
	return p, nil
}

func (g *stateTrackingGroup) track(p edge.FieldsTagsTimeSetter) error {
	pass, err := EvalPredicate(g.Expression, g.n.scopePool, p)
	if err != nil {
		return err
	}

	fields := p.Fields().Copy()
	fields[g.n.as] = g.tracker.track(p.Time(), pass)
	p.SetFields(fields)
	return nil
}

func (g *stateTrackingGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (g *stateTrackingGroup) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (g *stateTrackingGroup) Done() {}

type stateDurationTracker struct {
	sd *pipeline.StateDurationNode

	startTime time.Time
}

func (sdt *stateDurationTracker) reset() {
	sdt.startTime = time.Time{}
}

func (sdt *stateDurationTracker) track(t time.Time, inState bool) interface{} {
	if !inState {
		sdt.startTime = time.Time{}
		return float64(-1)
	}

	if sdt.startTime.IsZero() {
		sdt.startTime = t
	}
	return float64(t.Sub(sdt.startTime)) / float64(sdt.sd.Unit)
}

func newStateDurationNode(et *ExecutingTask, sd *pipeline.StateDurationNode, d NodeDiagnostic) (*StateTrackingNode, error) {
	if sd.Lambda == nil {
		return nil, fmt.Errorf("nil expression passed to StateDurationNode")
	}
	// Validate lambda expression
	expr, err := stateful.NewExpression(sd.Lambda.Expression)
	if err != nil {
		return nil, err
	}
	n := &StateTrackingNode{
		node:       node{Node: sd, et: et, diag: d},
		as:         sd.As,
		newTracker: func() stateTracker { return &stateDurationTracker{sd: sd} },
		expr:       expr,
		scopePool:  stateful.NewScopePool(ast.FindReferenceVariables(sd.Lambda.Expression)),
	}
	n.node.runF = n.runStateTracking
	return n, nil
}

type stateCountTracker struct {
	count int64
}

func (sct *stateCountTracker) reset() {
	sct.count = 0
}

func (sct *stateCountTracker) track(t time.Time, inState bool) interface{} {
	if !inState {
		sct.count = 0
		return int64(-1)
	}

	sct.count++
	return sct.count
}

func newStateCountNode(et *ExecutingTask, sc *pipeline.StateCountNode, d NodeDiagnostic) (*StateTrackingNode, error) {
	if sc.Lambda == nil {
		return nil, fmt.Errorf("nil expression passed to StateCountNode")
	}
	// Validate lambda expression
	expr, err := stateful.NewExpression(sc.Lambda.Expression)
	if err != nil {
		return nil, err
	}
	n := &StateTrackingNode{
		node:       node{Node: sc, et: et, diag: d},
		as:         sc.As,
		newTracker: func() stateTracker { return &stateCountTracker{} },
		expr:       expr,
		scopePool:  stateful.NewScopePool(ast.FindReferenceVariables(sc.Lambda.Expression)),
	}
	n.node.runF = n.runStateTracking
	return n, nil
}
