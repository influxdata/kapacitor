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

type MapFunc func(itr tsdb.Iterator) interface{}
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
	for b := s.ins[0].NextBatch(); b != nil; b = s.ins[0].NextBatch() {
		mr := &MapResult{
			Outs: make([]interface{}, s.parallel),
		}
		itr := newItr(b, s.mr.Fields[0])
		if len(b) == 0 {
			continue
		}

		l := len(b) - 1
		mr.Name = b[l].Name
		mr.Tags = b[l].Tags
		mr.Time = b[l].Time
		mr.Group = b[l].Group

		for i := 0; i < s.parallel; i++ {
			go func(i int) {
				mr.Outs[i] = s.f(itr)
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
	for m := s.ins[0].NextMaps(); m != nil; m = s.ins[0].NextMaps() {
		field, v := s.f(m.Outs)
		p := models.NewPoint(
			m.Name,
			m.Group,
			m.Tags,
			map[string]interface{}{field: v},
			m.Time,
		)
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
