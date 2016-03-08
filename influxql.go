package kapacitor

import (
	"fmt"
	"log"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

// tmpl -- go get github.com/benbjohnson/tmpl
//go:generate tmpl -data=@tmpldata influxql.gen.go.tmpl

type createReduceContextFunc func(c baseReduceContext) reduceContext

type InfluxQLNode struct {
	node
	n        *pipeline.InfluxQLNode
	createFn createReduceContextFunc
}

func newInfluxQLNode(et *ExecutingTask, n *pipeline.InfluxQLNode, l *log.Logger) (*InfluxQLNode, error) {
	m := &InfluxQLNode{
		node: node{Node: n, et: et, logger: l},
		n:    n,
	}
	m.node.runF = m.runInfluxQLs
	return m, nil
}

func (n *InfluxQLNode) runInfluxQLs([]byte) error {
	switch n.n.Wants() {
	case pipeline.StreamEdge:
		return n.runStreamInfluxQL()
	case pipeline.BatchEdge:
		return n.runBatchInfluxQL()
	default:
		return fmt.Errorf("cannot map %v edge", n.n.Wants())
	}
}

type reduceContext interface {
	AggregatePoint(p *models.Point)
	AggregateBatch(b *models.Batch)
	EmitPoint() (models.Point, error)
	EmitBatch() models.Batch
	Time() time.Time
}

type baseReduceContext struct {
	as            string
	field         string
	name          string
	group         models.GroupID
	dimensions    models.Dimensions
	tags          models.Tags
	time          time.Time
	pointTimes    bool
	topBottomInfo *pipeline.TopBottomCallInfo
}

func (c *baseReduceContext) Time() time.Time {
	return c.time
}

func (n *InfluxQLNode) runStreamInfluxQL() error {
	contexts := make(map[models.GroupID]reduceContext)
	for p, ok := n.ins[0].NextPoint(); ok; {
		context := contexts[p.Group]
		// Fisrt point in window
		if context == nil {
			// Create new context
			c := baseReduceContext{
				as:         n.n.As,
				field:      n.n.Field,
				name:       p.Name,
				group:      p.Group,
				dimensions: p.Dimensions,
				tags:       p.PointTags(),
				time:       p.Time,
				pointTimes: n.n.PointTimes,
			}

			createFn, err := n.getCreateFn(p.Fields[c.field])
			if err != nil {
				return err
			}

			context = createFn(c)
			contexts[p.Group] = context
			context.AggregatePoint(&p)

		} else if p.Time.Equal(context.Time()) {
			context.AggregatePoint(&p)
			// advance to next point
			p, ok = n.ins[0].NextPoint()
		} else {
			err := n.emit(context)
			if err != nil {
				return err
			}

			// Nil out reduced point
			contexts[p.Group] = nil
			// do not advance,
			// go through loop again to initialize new iterator.
		}
	}
	return nil
}

func (n *InfluxQLNode) runBatchInfluxQL() error {
	for b, ok := n.ins[0].NextBatch(); ok; b, ok = n.ins[0].NextBatch() {
		// Skip empty batches
		if len(b.Points) == 0 {
			continue
		}

		// Create new base context
		c := baseReduceContext{
			as:         n.n.As,
			field:      n.n.Field,
			name:       b.Name,
			group:      b.Group,
			dimensions: b.PointDimensions(),
			tags:       b.Tags,
			time:       b.TMax,
			pointTimes: n.n.PointTimes,
		}
		createFn, err := n.getCreateFn(b.Points[0].Fields[c.field])
		if err != nil {
			return err
		}

		context := createFn(c)
		context.AggregateBatch(&b)
		err = n.emit(context)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *InfluxQLNode) getCreateFn(value interface{}) (createReduceContextFunc, error) {
	if n.createFn != nil {
		return n.createFn, nil
	}
	createFn, err := determineReduceContextCreateFn(n.n.Method, value, n.n.ReduceCreater)
	if err != nil {
		return nil, err
	}
	n.createFn = createFn
	return n.createFn, nil
}

func (n *InfluxQLNode) emit(context reduceContext) error {
	switch n.Provides() {
	case pipeline.StreamEdge:
		p, err := context.EmitPoint()
		if err != nil {
			return err
		}
		for _, out := range n.outs {
			err := out.CollectPoint(p)
			if err != nil {
				return err
			}
		}
	case pipeline.BatchEdge:
		b := context.EmitBatch()
		for _, out := range n.outs {
			err := out.CollectBatch(b)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
