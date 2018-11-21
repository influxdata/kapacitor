package kapacitor

import (
	"fmt"
	"reflect"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/pkg/errors"
)

// tmpl -- go get github.com/benbjohnson/tmpl
//go:generate tmpl -data=@tmpldata.json influxql.gen.go.tmpl

type createReduceContextFunc func(c baseReduceContext) reduceContext

type InfluxQLNode struct {
	node
	n                      *pipeline.InfluxQLNode
	createFn               createReduceContextFunc
	isStreamTransformation bool

	currentKind reflect.Kind
}

func newInfluxQLNode(et *ExecutingTask, n *pipeline.InfluxQLNode, d NodeDiagnostic) (*InfluxQLNode, error) {
	m := &InfluxQLNode{
		node:                   node{Node: n, et: et, diag: d},
		n:                      n,
		isStreamTransformation: n.ReduceCreater.IsStreamTransformation,
	}
	m.node.runF = m.runInfluxQL
	return m, nil
}

type reduceContext interface {
	AggregatePoint(name string, p edge.FieldsTagsTimeGetter) error
	EmitPoint() (edge.PointMessage, error)
	EmitBatch() edge.BufferedBatchMessage
}

type baseReduceContext struct {
	as            string
	field         string
	name          string
	groupInfo     edge.GroupInfo
	time          time.Time
	pointTimes    bool
	topBottomInfo *pipeline.TopBottomCallInfo
}

func (n *InfluxQLNode) runInfluxQL([]byte) error {
	consumer := edge.NewGroupedConsumer(
		n.ins[0],
		n,
	)
	n.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())
	return consumer.Consume()
}

func (n *InfluxQLNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	return edge.NewReceiverFromForwardReceiverWithStats(
		n.outs,
		edge.NewTimedForwardReceiver(n.timer, n.newGroup(first)),
	), nil
}

func (n *InfluxQLNode) newGroup(first edge.PointMeta) edge.ForwardReceiver {
	bc := baseReduceContext{
		as:         n.n.As,
		field:      n.n.Field,
		name:       first.Name(),
		groupInfo:  first.GroupInfo(),
		time:       first.Time(),
		pointTimes: n.n.PointTimes || n.isStreamTransformation,
	}
	g := influxqlGroup{
		n:  n,
		bc: bc,
	}
	if n.isStreamTransformation {
		return &influxqlStreamingTransformGroup{
			influxqlGroup: g,
		}
	}
	return &g
}

type influxqlGroup struct {
	n *InfluxQLNode

	bc baseReduceContext
	rc reduceContext

	batchSize int
	name      string
	begin     edge.BeginBatchMessage
}

func (g *influxqlGroup) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	g.begin = begin
	g.batchSize = 0
	g.bc.time = begin.Time()
	g.rc = nil
	return nil, nil
}

func (g *influxqlGroup) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	if g.rc == nil {
		if err := g.realizeReduceContextFromFields(bp.Fields()); err != nil {
			g.n.diag.Error("failed to realize reduce context from fields", err)
			return nil, nil
		}
	}
	g.batchSize++
	if err := g.rc.AggregatePoint(g.begin.Name(), bp); err != nil {
		g.n.diag.Error("failed to aggregate point in batch", err)
	}
	return nil, nil
}

func (g *influxqlGroup) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	if g.batchSize == 0 && !g.n.n.ReduceCreater.IsEmptyOK {
		// Do not call Emit on the reducer since it can't handle empty batches.
		return nil, nil
	}
	if g.rc == nil {
		// Assume float64 type since we do not have any data.
		if err := g.realizeReduceContext(reflect.Float64); err != nil {
			return nil, err
		}
	}
	m, err := g.n.emit(g.rc)
	if err != nil {
		g.n.diag.Error("failed to emit batch", err)
		return nil, nil
	}
	return m, nil
}

func (g *influxqlGroup) Point(p edge.PointMessage) (edge.Message, error) {
	if p.Time().Equal(g.bc.time) {
		g.aggregatePoint(p)
	} else {
		// Time has elapsed, emit current context
		var msg edge.Message
		if g.rc != nil {
			m, err := g.n.emit(g.rc)
			if err != nil {
				g.n.diag.Error("failed to emit stream", err)
			}
			msg = m
		}

		// Reset context
		g.bc.name = p.Name()
		g.bc.time = p.Time()
		g.rc = nil

		// Aggregate the current point
		g.aggregatePoint(p)

		return msg, nil
	}
	return nil, nil
}

func (g *influxqlGroup) aggregatePoint(p edge.PointMessage) {
	if g.rc == nil {
		if err := g.realizeReduceContextFromFields(p.Fields()); err != nil {
			g.n.diag.Error("failed to realize reduce context from fields", err)
			return
		}
	}
	err := g.rc.AggregatePoint(p.Name(), p)
	if err != nil {
		g.n.diag.Error("failed to aggregate point", err)
	}
}

func (g *influxqlGroup) getFieldKind(fields models.Fields) (reflect.Kind, error) {
	f, exists := fields[g.bc.field]
	if !exists {
		return reflect.Invalid, fmt.Errorf("field %q missing from point", g.bc.field)
	}

	return reflect.TypeOf(f).Kind(), nil
}
func (g *influxqlGroup) realizeReduceContextFromFields(fields models.Fields) error {
	k, err := g.getFieldKind(fields)
	if err != nil {
		return err
	}
	return g.realizeReduceContext(k)
}

func (g *influxqlGroup) realizeReduceContext(kind reflect.Kind) error {
	createFn, err := g.n.getCreateFn(kind)
	if err != nil {
		return err
	}
	g.rc = createFn(g.bc)
	return nil
}

func (g *influxqlGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (g *influxqlGroup) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (g *influxqlGroup) Done() {}

type influxqlStreamingTransformGroup struct {
	influxqlGroup
}

func (g *influxqlStreamingTransformGroup) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	g.begin = begin.ShallowCopy()
	g.begin.SetSizeHint(0)
	g.bc.time = begin.Time()
	g.rc = nil
	return begin, nil
}

func (g *influxqlStreamingTransformGroup) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	if g.rc == nil {
		if err := g.realizeReduceContextFromFields(bp.Fields()); err != nil {
			g.n.diag.Error("failed to realize reduce context from fields", err)
			return nil, nil
		}
	}
	if err := g.rc.AggregatePoint(g.begin.Name(), bp); err != nil {
		g.n.diag.Error("failed to aggregate batch point", err)
	}
	if ep, err := g.rc.EmitPoint(); err != nil {
		g.n.diag.Error("failed to emit batch point", err)
	} else if ep != nil {
		return edge.NewBatchPointMessage(
			ep.Fields(),
			ep.Tags(),
			ep.Time(),
		), nil
	}
	return nil, nil
}

func (g *influxqlStreamingTransformGroup) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return end, nil
}

func (g *influxqlStreamingTransformGroup) Point(p edge.PointMessage) (edge.Message, error) {
	if g.rc == nil {
		if err := g.realizeReduceContextFromFields(p.Fields()); err != nil {
			g.n.diag.Error("failed to realize reduce context from fields", err)
			// Skip point
			return nil, nil
		}
	}
	err := g.rc.AggregatePoint(p.Name(), p)
	if err != nil {
		g.n.diag.Error("failed to aggregate point", err)
	}

	m, err := g.n.emit(g.rc)
	if err != nil {
		g.n.diag.Error("failed to emit stream", err)
		return nil, nil
	}
	return m, nil
}

func (g *influxqlStreamingTransformGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}

func (n *InfluxQLNode) getCreateFn(kind reflect.Kind) (createReduceContextFunc, error) {
	changed := n.currentKind != kind
	if !changed && n.createFn != nil {
		return n.createFn, nil
	}
	n.currentKind = kind
	createFn, err := determineReduceContextCreateFn(n.n.Method, kind, n.n.ReduceCreater)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid influxql func %s with field %s", n.n.Method, n.n.Field)
	}
	n.createFn = createFn
	return n.createFn, nil
}

func (n *InfluxQLNode) emit(context reduceContext) (edge.Message, error) {
	switch n.Provides() {
	case pipeline.StreamEdge:
		return context.EmitPoint()
	case pipeline.BatchEdge:
		return context.EmitBatch(), nil
	}
	return nil, nil
}
