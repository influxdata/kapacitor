package kapacitor

import (
	"log"
	"sort"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

type FlattenNode struct {
	node
	f *pipeline.FlattenNode
}

// Create a new FlattenNode, which takes pairs from parent streams combines them into a single point.
func newFlattenNode(et *ExecutingTask, n *pipeline.FlattenNode, l *log.Logger) (*FlattenNode, error) {
	fn := &FlattenNode{
		f:    n,
		node: node{Node: n, et: et, logger: l},
	}
	fn.node.runF = fn.runFlatten
	return fn, nil
}

func (n *FlattenNode) runFlatten([]byte) error {
	switch n.Wants() {
	case pipeline.StreamEdge:
		buffers := make(map[models.GroupID]*buffer)
		for p, ok := n.ins[0].NextPoint(); ok; p, ok = n.ins[0].NextPoint() {
			n.timer.Start()
			t := p.Time.Round(n.c.Tolerance)
			currentBuf, ok := buffers[p.Group]
			if !ok {
				currentBuf = &buffer{
					Time:       t,
					Name:       p.Name,
					Group:      p.Group,
					Dimensions: p.Dimensions,
				}
				buffers[p.Group] = currentBuf
			}
			rp := rawPoint{
				Time:   t,
				Fields: p.Fields,
				Tags:   p.Tags,
			}
			if t.Equal(currentBuf.Time) {
				currentBuf.Points = append(currentBuf.Points, rp)
			} else {
				if err := n.flattenBuffer(currentBuf); err != nil {
					return err
				}
				currentBuf.Time = t
				currentBuf.Name = p.Name
				currentBuf.Group = p.Group
				currentBuf.Dimensions = p.Dimensions
				currentBuf.Points = currentBuf.Points[0:1]
				currentBuf.Points[0] = rp
			}
			n.timer.Stop()
		}
	case pipeline.BatchEdge:
		allBuffers := make(map[models.GroupID]map[time.Time]*buffer)
		groupTimes := make(map[models.GroupID]time.Time)
		for b, ok := n.ins[0].NextBatch(); ok; b, ok = n.ins[0].NextBatch() {
			n.timer.Start()
			t := b.TMax.Round(n.c.Tolerance)
			buffers, ok := allBuffers[b.Group]
			if !ok {
				buffers = make(map[time.Time]*buffer)
				allBuffers[b.Group] = buffers
				groupTimes[b.Group] = t
			}
			groupTime := groupTimes[b.Group]
			if !t.Equal(groupTime) {
				// Set new groupTime
				groupTimes[b.Group] = t
				// Combine/Emit all old buffers
				times := make(timeList, 0, len(buffers))
				for t := range buffers {
					times = append(times, t)
				}
				sort.Sort(times)
				for _, t := range times {
					if err := n.combineBuffer(buffers[t]); err != nil {
						return err
					}
					delete(buffers, t)
				}
			}
			for _, p := range b.Points {
				t := p.Time.Round(n.c.Tolerance)
				currentBuf, ok := buffers[t]
				if !ok {
					currentBuf = &buffer{
						Time:       t,
						Name:       b.Name,
						Group:      b.Group,
						Dimensions: b.PointDimensions(),
					}
					buffers[t] = currentBuf
				}
				currentBuf.Points = append(currentBuf.Points, rawPoint{
					Time:   t,
					Fields: p.Fields,
					Tags:   p.Tags,
				})
			}
			n.timer.Stop()
		}
	}
	return nil

}

func (n *FlattenNode) flattenBuffer(buf *buffer) error {
}
