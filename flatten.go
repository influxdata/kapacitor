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

type flattenStreamBuffer struct {
	Time       time.Time
	Name       string
	Group      models.GroupID
	Dimensions models.Dimensions
	Tags       models.Tags
	Points     []models.RawPoint
}

type flattenBatchBuffer struct {
	Time   time.Time
	Name   string
	Group  models.GroupID
	Tags   models.Tags
	Points map[time.Time][]models.RawPoint
}

func (n *FlattenNode) runFlatten([]byte) error {
	switch n.Wants() {
	case pipeline.StreamEdge:
		flattenBuffers := make(map[models.GroupID]*flattenStreamBuffer)
		for p, ok := n.ins[0].NextPoint(); ok; p, ok = n.ins[0].NextPoint() {
			n.timer.Start()
			t := p.Time.Round(n.f.Tolerance)
			currentBuf, ok := flattenBuffers[p.Group]
			if !ok {
				currentBuf = &flattenStreamBuffer{
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
				if fields, err := n.flatten(currentBuf.Points); err != nil {
					return err
				} else {
					// Emit point
					flatP := models.Point{
						Time:       currentBuf.Time,
						Name:       currentBuf.Name,
						Group:      currentBuf.Group,
						Dimensions: currentBuf.Dimensions,
						Tags:       currentBuf.Tags,
						Fields:     fields,
					}
					n.timer.Pause()
					for _, out := range n.outs {
						err := out.CollectPoint(flatP)
						if err != nil {
							return err
						}
					}
					n.timer.Resume()
				}
				// Update currentBuf with new time and initial point
				currentBuf.Time = t
				currentBuf.Points = currentBuf.Points[0:1]
				currentBuf.Points[0] = rp
			}
			n.timer.Stop()
		}
	case pipeline.BatchEdge:
		allBuffers := make(map[models.GroupID]*flattenBatchBuffer)
		for b, ok := n.ins[0].NextBatch(); ok; b, ok = n.ins[0].NextBatch() {
			n.timer.Start()
			t := b.TMax.Round(n.f.Tolerance)
			currentBuf, ok := allBuffers[b.Group]
			if !ok {
				currentBuf = &flattenBatchBuffer{
					Time:   t,
					Name:   b.Name,
					Group:  b.Group,
					Tags:   b.Tags,
					Points: make(map[time.Time][]models.RawPoint),
				}
				allBuffers[b.Group] = currentBuf
			}
			if !t.Equal(currentBuf.Time) {
				// Flatten/Emit old buffer
				times := make(timeList, 0, len(currentBuf.Points))
				for t := range currentBuf.Points {
					times = append(times, t)
				}
				sort.Sort(times)
				flatBatch := models.Batch{
					TMax:   currentBuf.Time,
					Name:   currentBuf.Name,
					Group:  currentBuf.Group,
					ByName: b.ByName,
					Tags:   currentBuf.Tags,
				}
				for _, t := range times {
					if fields, err := n.flatten(currentBuf.Points[t]); err != nil {
						return err
					} else {
						flatBatch.Points = append(flatBatch.Points, models.BatchPoint{
							Time:   t,
							Tags:   currentBuf.Tags,
							Fields: fields,
						})
					}
					delete(currentBuf.Points, t)
				}
				n.timer.Pause()
				for _, out := range n.outs {
					err := out.CollectBatch(flatBatch)
					if err != nil {
						return err
					}
				}
				n.timer.Resume()
				// Update currentBuf with new time
				currentBuf.Time = t
			}
			for _, p := range b.Points {
				t := p.Time.Round(n.f.Tolerance)
				currentBuf.Points[t] = append(currentBuf.Points[t], models.RawPoint{
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

func (n *FlattenNode) flatten(points []models.RawPoint) (models.Fields, error) {
	fields := make(models.Fields)
	if len(points) == 0 {
		return fields, nil
	}
	fieldPrefix := n.bufPool.Get().(*bytes.Buffer)
	defer n.bufPool.Put(fieldPrefix)
POINTS:
	for _, p := range points {
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
			fields[fieldPrefix.String()] = value
			fieldPrefix.Truncate(l)
		}
		fieldPrefix.Reset()
	}
	return fields, nil
}
