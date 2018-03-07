package kapacitor

import (
	"errors"
	"fmt"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

type WhereNode struct {
	node
	w        *pipeline.WhereNode
	endpoint string

	expression stateful.Expression
	scopePool  stateful.ScopePool
}

// Create a new WhereNode which filters down the batch or stream by a condition
func newWhereNode(et *ExecutingTask, n *pipeline.WhereNode, d NodeDiagnostic) (wn *WhereNode, err error) {
	wn = &WhereNode{
		node: node{Node: n, et: et, diag: d},
		w:    n,
	}

	expr, err := stateful.NewExpression(n.Lambda.Expression)
	if err != nil {
		return nil, fmt.Errorf("Failed to compile expression in where clause: %v", err)
	}
	wn.expression = expr
	wn.scopePool = stateful.NewScopePool(ast.FindReferenceVariables(n.Lambda.Expression))

	wn.runF = wn.runWhere
	if n.Lambda == nil {
		return nil, errors.New("nil expression passed to WhereNode")
	}
	return
}

func (n *WhereNode) runWhere(snapshot []byte) error {
	consumer := edge.NewGroupedConsumer(
		n.ins[0],
		n,
	)
	n.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())

	return consumer.Consume()
}

func (n *WhereNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	return edge.NewReceiverFromForwardReceiverWithStats(
		n.outs,
		edge.NewTimedForwardReceiver(n.timer, n.newGroup()),
	), nil
}

func (n *WhereNode) newGroup() *whereGroup {
	return &whereGroup{
		n:    n,
		expr: n.expression.CopyReset(),
	}
}

type whereGroup struct {
	n    *WhereNode
	expr stateful.Expression
}

func (g *whereGroup) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	begin = begin.ShallowCopy()
	begin.SetSizeHint(0)
	return begin, nil
}

func (g *whereGroup) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	return g.doWhere(bp)
}

func (g *whereGroup) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return end, nil
}

func (g *whereGroup) Point(p edge.PointMessage) (edge.Message, error) {
	return g.doWhere(p)
}

func (g *whereGroup) doWhere(p edge.FieldsTagsTimeGetterMessage) (edge.Message, error) {
	pass, err := EvalPredicate(g.expr, g.n.scopePool, p)
	if err != nil {
		g.n.diag.Error("error while evaluating expression", err)
		return nil, nil
	}
	if pass {
		return p, nil
	}
	return nil, nil
}

func (g *whereGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (g *whereGroup) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (g *whereGroup) Done() {}
