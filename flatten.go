package kapacitor

import (
	"bytes"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

type FlattenNode struct {
	node
	f *pipeline.FlattenNode

	bufPool sync.Pool
}

// Create a new FlattenNode, which takes pairs from parent streams combines them into a single point.
func newFlattenNode(et *ExecutingTask, n *pipeline.FlattenNode, l *log.Logger) (*FlattenNode, error) {
	fn := &FlattenNode{
		f:    n,
		node: node{Node: n, et: et, logger: l},
		bufPool: sync.Pool{
			New: func() interface{} { return &bytes.Buffer{} },
		},
	}
	fn.node.runF = fn.runFlatten
	return fn, nil
}

type flattenBuffer struct {
	Time       time.Time
	Name       string
	Group      models.GroupID
	Dimensions models.Dimensions
	Tags       models.Tags
	Points     []models.RawPoint
}

func (n *FlattenNode) runFlatten([]byte) error {
	switch n.Wants() {
	case pipeline.StreamEdge:
		flattenBuffers := make(map[models.GroupID]*flattenBuffer)
		for p, ok := n.ins[0].NextPoint(); ok; p, ok = n.ins[0].NextPoint() {
			n.timer.Start()
			t := p.Time.Round(n.f.Tolerance)
			currentBuf, ok := flattenBuffers[p.Group]
			if !ok {
				currentBuf = &flattenBuffer{
					Time:       t,
					Name:       p.Name,
					Group:      p.Group,
					Dimensions: p.Dimensions,
					Tags:       p.PointTags(),
				}
				flattenBuffers[p.Group] = currentBuf
			}
			rp := models.RawPoint{
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
		allBuffers := make(map[models.GroupID]map[time.Time]*flattenBuffer)
		groupTimes := make(map[models.GroupID]time.Time)
		for b, ok := n.ins[0].NextBatch(); ok; b, ok = n.ins[0].NextBatch() {
			n.timer.Start()
			t := b.TMax.Round(n.f.Tolerance)
			flattenBuffers, ok := allBuffers[b.Group]
			if !ok {
				flattenBuffers = make(map[time.Time]*flattenBuffer)
				allBuffers[b.Group] = flattenBuffers
				groupTimes[b.Group] = t
			}
			groupTime := groupTimes[b.Group]
			if !t.Equal(groupTime) {
				// Set new groupTime
				groupTimes[b.Group] = t
				// Combine/Emit all old flattenBuffers
				times := make(timeList, 0, len(flattenBuffers))
				for t := range flattenBuffers {
					times = append(times, t)
				}
				sort.Sort(times)
				for _, t := range times {
					if err := n.flattenBuffer(flattenBuffers[t]); err != nil {
						return err
					}
					delete(flattenBuffers, t)
				}
			}
			for _, p := range b.Points {
				t := p.Time.Round(n.f.Tolerance)
				currentBuf, ok := flattenBuffers[t]
				if !ok {
					currentBuf = &flattenBuffer{
						Time:       t,
						Name:       b.Name,
						Group:      b.Group,
						Dimensions: b.PointDimensions(),
						Tags:       b.Tags,
					}
					flattenBuffers[t] = currentBuf
				}
				currentBuf.Points = append(currentBuf.Points, models.RawPoint{
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

func (n *FlattenNode) flattenBuffer(buf *flattenBuffer) error {
	if len(buf.Points) == 0 {
		return nil
	}
	flatP := models.Point{
		Name:       buf.Name,
		Group:      buf.Group,
		Dimensions: buf.Dimensions,
		Time:       buf.Time,
		Tags:       buf.Tags,
	}
	fieldPrefix := n.bufPool.Get().(*bytes.Buffer)
	defer n.bufPool.Put(fieldPrefix)
POINTS:
	for _, p := range buf.Points {
		for _, tag := range n.f.Dimensions {
			if v, ok := p.Tags[tag]; ok {
				fieldPrefix.WriteString(v)
				fieldPrefix.WriteString(n.f.Delimiter)
			} else {
				n.logger.Printf("E! point missing tag %q for flatten operation", tag)
				continue POINTS
			}
		}
		l := fieldPrefix.Len()
		for fname, value := range p.Fields {
			fieldPrefix.WriteString(fname)
			flatP.Fields[fieldPrefix.String()] = value
			fieldPrefix.Truncate(l)
		}
		fieldPrefix.Reset()
	}
	// Emit point
	n.timer.Pause()
	for _, out := range n.outs {
		err := out.CollectPoint(flatP)
		if err != nil {
			return err
		}
	}
	n.timer.Resume()
	return nil
}
