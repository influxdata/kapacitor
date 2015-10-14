package kapacitor

import (
	"fmt"
	"time"

	"github.com/influxdb/influxdb/tsdb"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

type MapResult struct {
	Name  string
	Group models.GroupID
	Tags  map[string]string
	Time  time.Time
	Outs  []interface{}
}

type MapFunc func(in *tsdb.MapInput) interface{}
type ReduceFunc func([]interface{}) (string, interface{})
type MapReduceFunc func() (MapFunc, ReduceFunc)

type MapNode struct {
	node
	mr       *pipeline.MapNode
	f        MapFunc
	parallel int
}

func newMapNode(et *ExecutingTask, n *pipeline.MapNode) (*MapNode, error) {
	f, ok := n.Func.(MapFunc)
	if !ok {
		return nil, fmt.Errorf("invalid func given to batch map node %T", n.Func)
	}
	s := &MapNode{
		node:     node{Node: n, et: et},
		mr:       n,
		f:        f,
		parallel: 2,
	}
	s.node.runF = s.runMaps
	return s, nil
}

func (s *MapNode) runMaps() error {
	done := make(chan bool, s.parallel)
	for b, ok := s.ins[0].NextBatch(); ok; b, ok = s.ins[0].NextBatch() {
		if len(b.Points) == 0 {
			continue
		}
		mr := &MapResult{
			Outs: make([]interface{}, s.parallel),
		}
		inputs := make([]tsdb.MapInput, s.parallel)
		j := 0
		for _, p := range b.Points {
			item := tsdb.MapItem{
				Timestamp: p.Time.Unix(),
				Value:     p.Fields[s.mr.Field],
				Fields:    p.Fields,
				Tags:      b.Tags,
			}
			inputs[j].Items = append(inputs[j].Items, item)
			if len(inputs[j].Items) == 1 {
				inputs[j].TMin = item.Timestamp
			}
			j = (j + 1) % s.parallel
		}

		l := len(b.Points) - 1
		mr.Name = b.Name
		mr.Group = b.Group
		mr.Tags = b.Tags

		mr.Time = b.Points[l].Time

		for i := 0; i < s.parallel; i++ {
			go func(i int) {
				mr.Outs[i] = s.f(&inputs[i])
				done <- true
			}(i)
		}

		finished := 0
		for finished != s.parallel {
			<-done
			finished++
		}

		for _, child := range s.outs {
			err := child.CollectMaps(mr)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type ReduceNode struct {
	node
	mr *pipeline.ReduceNode
	f  ReduceFunc
}

func newReduceNode(et *ExecutingTask, n *pipeline.ReduceNode) (*ReduceNode, error) {
	f, ok := n.Func.(ReduceFunc)
	if !ok {
		return nil, fmt.Errorf("invalid func given to batch reduce node %T", n.Func)
	}
	b := &ReduceNode{
		node: node{Node: n, et: et},
		mr:   n,
		f:    f,
	}
	b.node.runF = b.runReduce
	return b, nil
}

func (s *ReduceNode) runReduce() error {
	for m, ok := s.ins[0].NextMaps(); ok; m, ok = s.ins[0].NextMaps() {
		field, v := s.f(m.Outs)
		p := models.Point{
			Name:   m.Name,
			Group:  m.Group,
			Tags:   m.Tags,
			Fields: models.Fields{field: v},
			Time:   m.Time,
		}
		for _, c := range s.outs {
			err := c.CollectPoint(p)
			if err != nil {
				return err
			}
		}
	}
	s.closeChildEdges()
	return nil
}
