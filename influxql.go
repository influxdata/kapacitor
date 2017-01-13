package kapacitor

import (
	"fmt"
	"log"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/pkg/errors"
)

// tmpl -- go get github.com/benbjohnson/tmpl
//go:generate tmpl -data=@tmpldata influxql.gen.go.tmpl

type createReduceContextFunc func(c baseReduceContext) reduceContext

var ErrEmptyEmit = errors.New("error call to emit produced no results")

type InfluxQLNode struct {
	node
	n                      *pipeline.InfluxQLNode
	createFn               createReduceContextFunc
	isStreamTransformation bool
}

func newInfluxQLNode(et *ExecutingTask, n *pipeline.InfluxQLNode, l *log.Logger) (*InfluxQLNode, error) {
	m := &InfluxQLNode{
		node: node{Node: n, et: et, logger: l},
		n:    n,
		isStreamTransformation: n.ReduceCreater.IsStreamTransformation,
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
	AggregatePoint(p *models.Point) error
	AggregateBatch(b *models.Batch) error
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
		n.timer.Start()
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
				pointTimes: n.n.PointTimes || n.isStreamTransformation,
			}

			createFn, err := n.getCreateFn(p.Fields[c.field])
			if err != nil {
				return err
			}

			context = createFn(c)
			contexts[p.Group] = context

		}
		if n.isStreamTransformation {
			err := context.AggregatePoint(&p)
			if err != nil {
				n.logger.Println("E! failed to aggregate point:", err)
			}
			p, ok = n.ins[0].NextPoint()

			err = n.emit(context)
			if err != nil && err != ErrEmptyEmit {
				n.logger.Println("E! failed to emit stream:", err)
			}
		} else {
			if p.Time.Equal(context.Time()) {
				err := context.AggregatePoint(&p)
				if err != nil {
					n.logger.Println("E! failed to aggregate point:", err)
				}
				// advance to next point
				p, ok = n.ins[0].NextPoint()
			} else {
				err := n.emit(context)
				if err != nil {
					n.logger.Println("E! failed to emit stream:", err)
				}

				// Nil out reduced point
				contexts[p.Group] = nil
				// do not advance,
				// go through loop again to initialize new iterator.
			}
		}
		n.timer.Stop()
	}
	return nil
}

func (n *InfluxQLNode) runBatchInfluxQL() error {
	var exampleValue interface{}
	for b, ok := n.ins[0].NextBatch(); ok; b, ok = n.ins[0].NextBatch() {
		n.timer.Start()
		// Create new base context
		c := baseReduceContext{
			as:         n.n.As,
			field:      n.n.Field,
			name:       b.Name,
			group:      b.Group,
			dimensions: b.PointDimensions(),
			tags:       b.Tags,
			time:       b.TMax,
			pointTimes: n.n.PointTimes || n.isStreamTransformation,
		}
		if len(b.Points) == 0 {
			if !n.n.ReduceCreater.IsEmptyOK {
				// If the reduce does not handle empty batches continue
				n.timer.Stop()
				continue
			}
			if exampleValue == nil {
				// If we have no points and have never seen a point assume float64
				exampleValue = float64(0)
			}
		} else {
			exampleValue = b.Points[0].Fields[c.field]
		}
		createFn, err := n.getCreateFn(exampleValue)
		if err != nil {
			return err
		}

		context := createFn(c)
		if n.isStreamTransformation {
			// We have a stream transformation, so treat the batch as if it were a stream
			// Create a new batch for emitting
			eb := b
			eb.Points = make([]models.BatchPoint, 0, len(b.Points))
			for _, bp := range b.Points {
				p := models.Point{
					Name:   b.Name,
					Time:   bp.Time,
					Fields: bp.Fields,
					Tags:   bp.Tags,
				}
				if err := context.AggregatePoint(&p); err != nil {
					n.logger.Println("E! failed to aggregate batch point:", err)
				}
				if ep, err := context.EmitPoint(); err != nil && err != ErrEmptyEmit {
					n.logger.Println("E! failed to emit batch point:", err)
				} else if err != ErrEmptyEmit {
					eb.Points = append(eb.Points, models.BatchPoint{
						Time:   ep.Time,
						Fields: ep.Fields,
						Tags:   ep.Tags,
					})
				}
			}
			// Emit the complete batch
			n.timer.Pause()
			for _, out := range n.outs {
				if err := out.CollectBatch(eb); err != nil {
					n.logger.Println("E! failed to emit batch points:", err)
				}
			}
			n.timer.Resume()
		} else {
			err := context.AggregateBatch(&b)
			if err == nil {
				if err := n.emit(context); err != nil {
					n.logger.Println("E! failed to emit batch:", err)
				}
			} else {
				n.logger.Println("E! failed to aggregate batch:", err)
			}
		}
		n.timer.Stop()
	}
	return nil
}

func (n *InfluxQLNode) getCreateFn(value interface{}) (createReduceContextFunc, error) {
	if n.createFn != nil {
		return n.createFn, nil
	}
	createFn, err := determineReduceContextCreateFn(n.n.Method, value, n.n.ReduceCreater)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid influxql func %s with field %s", n.n.Method, n.n.Field)
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
		n.timer.Pause()
		for _, out := range n.outs {
			err := out.CollectPoint(p)
			if err != nil {
				return err
			}
		}
		n.timer.Resume()
	case pipeline.BatchEdge:
		b := context.EmitBatch()
		n.timer.Pause()
		for _, out := range n.outs {
			err := out.CollectBatch(b)
			if err != nil {
				return err
			}
		}
		n.timer.Resume()
	}
	return nil
}
