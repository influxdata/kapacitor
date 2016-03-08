package kapacitor

import (
	"fmt"
	"log"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdb/influxdb/tsdb"
)

type MapResult struct {
	Name  string
	Group models.GroupID
	Dims  []string
	Tags  map[string]string
	TMax  time.Time
	Outs  []interface{}
}

type MapFunc func(in *tsdb.MapInput) interface{}
type ReduceFunc func(in []interface{}, tmax time.Time, useTMax bool, as string) interface{}

type MapInfo struct {
	Field string
	Func  MapFunc
}

type MapNode struct {
	node
	mr       *pipeline.MapNode
	f        MapFunc
	field    string
	parallel int
}

func newMapNode(et *ExecutingTask, n *pipeline.MapNode, l *log.Logger) (*MapNode, error) {
	l.Println("W! DEPRECATED use InfluxQLNode instead")
	f, ok := n.Map.(MapInfo)
	if !ok {
		return nil, fmt.Errorf("invalid map given to map node %T", n.Map)
	}
	m := &MapNode{
		node:     node{Node: n, et: et, logger: l},
		mr:       n,
		f:        f.Func,
		field:    f.Field,
		parallel: 2,
	}
	m.node.runF = m.runMaps
	return m, nil
}

func (m *MapNode) runMaps([]byte) error {
	switch m.mr.Wants() {
	case pipeline.StreamEdge:
		return m.runStreamMap()
	case pipeline.BatchEdge:
		return m.runBatchMap()
	default:
		return fmt.Errorf("cannot map %v edge", m.mr.Wants())
	}
}

func (m *MapNode) runStreamMap() error {
	batches := make(map[models.GroupID]*models.Batch)
	for p, ok := m.ins[0].NextPoint(); ok; {
		b := batches[p.Group]
		if b == nil {
			// Create new batch
			tags := make(map[string]string, len(p.Dimensions))
			for _, dim := range p.Dimensions {
				tags[dim] = p.Tags[dim]
			}
			b = &models.Batch{
				Name:  p.Name,
				Group: p.Group,
				Tags:  tags,
				TMax:  p.Time,
			}
			batches[p.Group] = b
		}
		if p.Time.Equal(b.TMax) {
			b.Points = append(b.Points, models.BatchPointFromPoint(p))
			// advance to next point
			p, ok = m.ins[0].NextPoint()
		} else {
			err := m.mapBatch(*b)
			if err != nil {
				return err
			}
			batches[b.Group] = nil
		}
	}
	return nil
}

func (m *MapNode) runBatchMap() error {
	for b, ok := m.ins[0].NextBatch(); ok; b, ok = m.ins[0].NextBatch() {
		err := m.mapBatch(b)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MapNode) mapBatch(b models.Batch) error {
	if len(b.Points) == 0 {
		return nil
	}
	m.timer.Start()
	done := make(chan bool, m.parallel)
	mr := &MapResult{
		Outs: make([]interface{}, m.parallel),
	}
	inputs := make([]tsdb.MapInput, m.parallel)
	j := 0
	for _, p := range b.Points {
		value, ok := p.Fields[m.field]
		if !ok {
			fields := make([]string, 0, len(p.Fields))
			for field := range p.Fields {
				fields = append(fields, field)
			}
			return fmt.Errorf("unknown field %s, available fields %v", m.field, fields)
		}
		item := tsdb.MapItem{
			Timestamp: p.Time.Unix(),
			Value:     value,
			Fields:    p.Fields,
			Tags:      p.Tags,
		}
		inputs[j].Items = append(inputs[j].Items, item)
		inputs[j].TMin = -1
		j = (j + 1) % m.parallel
	}

	mr.Name = b.Name
	mr.Group = b.Group
	mr.TMax = b.TMax
	mr.Tags = b.Tags
	mr.Dims = models.SortedKeys(b.Tags)

	for i := 0; i < m.parallel; i++ {
		go func(i int) {
			mr.Outs[i] = m.f(&inputs[i])
			done <- true
		}(i)
	}

	finished := 0
	for finished != m.parallel {
		<-done
		finished++
	}

	m.timer.Stop()
	for _, child := range m.outs {
		err := child.CollectMaps(mr)
		if err != nil {
			return err
		}
	}
	return nil
}

type ReduceNode struct {
	node
	r *pipeline.ReduceNode
	f ReduceFunc
}

func newReduceNode(et *ExecutingTask, n *pipeline.ReduceNode, l *log.Logger) (*ReduceNode, error) {
	f, ok := n.Reduce.(ReduceFunc)
	if !ok {
		return nil, fmt.Errorf("invalid func given to batch reduce node %T", n.Reduce)
	}
	b := &ReduceNode{
		node: node{Node: n, et: et, logger: l},
		r:    n,
		f:    f,
	}
	b.node.runF = b.runReduce
	return b, nil
}

func (r *ReduceNode) runReduce([]byte) error {
	for m, ok := r.ins[0].NextMaps(); ok; m, ok = r.ins[0].NextMaps() {
		r.timer.Start()
		rr := r.f(m.Outs, m.TMax, r.r.PointTimes, r.r.As)
		switch result := rr.(type) {
		case models.Point:
			result.Name = m.Name
			result.Group = m.Group
			result.Dimensions = m.Dims
			result.Tags = m.Tags
			r.timer.Stop()
			for _, c := range r.outs {
				err := c.CollectPoint(result)
				if err != nil {
					return err
				}
			}
		case models.Batch:
			result.Name = m.Name
			result.Group = m.Group
			result.Tags = m.Tags
			result.TMax = m.TMax
			r.timer.Stop()
			for _, c := range r.outs {
				err := c.CollectBatch(result)
				if err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unexpected result from reduce function %T", rr)
		}
	}
	return nil
}
