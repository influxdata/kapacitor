package kapacitor

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

type FlattenNode struct {
	node
	f *pipeline.FlattenNode

	bufPool sync.Pool
}

// Create a new FlattenNode, which takes pairs from parent streams combines them into a single point.
func newFlattenNode(et *ExecutingTask, n *pipeline.FlattenNode, d NodeDiagnostic) (*FlattenNode, error) {
	fn := &FlattenNode{
		f:    n,
		node: node{Node: n, et: et, diag: d},
		bufPool: sync.Pool{
			New: func() interface{} { return &bytes.Buffer{} },
		},
	}
	fn.node.runF = fn.runFlatten
	return fn, nil
}

func (n *FlattenNode) runFlatten([]byte) error {
	consumer := edge.NewGroupedConsumer(
		n.ins[0],
		n,
	)
	n.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())
	return consumer.Consume()
}

func (n *FlattenNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	t := first.Time().Round(n.f.Tolerance)
	return &flattenBuffer{
		n:         n,
		time:      t,
		name:      first.Name(),
		groupInfo: group,
	}, nil
}

type flattenBuffer struct {
	n         *FlattenNode
	time      time.Time
	name      string
	groupInfo edge.GroupInfo
	points    []edge.FieldsTagsTimeGetter
}

func (b *flattenBuffer) BeginBatch(begin edge.BeginBatchMessage) error {
	b.n.timer.Start()
	defer b.n.timer.Stop()

	b.name = begin.Name()
	b.time = time.Time{}
	if s := begin.SizeHint(); s > cap(b.points) {
		b.points = make([]edge.FieldsTagsTimeGetter, 0, s)
	}

	begin = begin.ShallowCopy()
	begin.SetSizeHint(0)
	b.n.timer.Pause()
	err := edge.Forward(b.n.outs, begin)
	b.n.timer.Resume()
	return err
}

func (b *flattenBuffer) BatchPoint(bp edge.BatchPointMessage) error {
	b.n.timer.Start()
	defer b.n.timer.Stop()

	t := bp.Time().Round(b.n.f.Tolerance)
	bp = bp.ShallowCopy()
	bp.SetTime(t)

	t, fields, err := b.addPoint(bp)
	if err != nil {
		return err
	}
	if len(fields) == 0 {
		return nil
	}

	return b.emitBatchPoint(t, fields)
}

func (b *flattenBuffer) emitBatchPoint(t time.Time, fields models.Fields) error {
	// Emit batch point
	flatP := edge.NewBatchPointMessage(
		fields,
		b.groupInfo.Tags,
		t,
	)
	b.n.timer.Pause()
	err := edge.Forward(b.n.outs, flatP)
	b.n.timer.Resume()
	return err
}

func (b *flattenBuffer) EndBatch(end edge.EndBatchMessage) error {
	b.n.timer.Start()
	defer b.n.timer.Stop()

	if len(b.points) > 0 {
		fields, err := b.n.flatten(b.points)
		if err != nil {
			return err
		}
		if err := b.emitBatchPoint(b.time, fields); err != nil {
			return err
		}
		b.points = b.points[0:0]
	}

	b.n.timer.Pause()
	err := edge.Forward(b.n.outs, end)
	b.n.timer.Resume()
	return err
}

func (b *flattenBuffer) Point(p edge.PointMessage) error {
	b.n.timer.Start()
	defer b.n.timer.Stop()

	t := p.Time().Round(b.n.f.Tolerance)
	p = p.ShallowCopy()
	p.SetTime(t)

	t, fields, err := b.addPoint(p)
	if err != nil {
		return err
	}
	if len(fields) == 0 {
		return nil
	}

	// Emit point
	flatP := edge.NewPointMessage(
		b.name, "", "",
		b.groupInfo.Dimensions,
		fields,
		b.groupInfo.Tags,
		t,
	)
	b.n.timer.Pause()
	err = edge.Forward(b.n.outs, flatP)
	b.n.timer.Resume()
	return err
}

func (b *flattenBuffer) addPoint(p edge.FieldsTagsTimeGetter) (next time.Time, fields models.Fields, err error) {
	t := p.Time()
	if !t.Equal(b.time) {
		if len(b.points) > 0 {
			fields, err = b.n.flatten(b.points)
			if err != nil {
				return
			}
			next = b.time
			b.points = b.points[0:0]
		}
		// Update buffer with new time
		b.time = t
	}
	b.points = append(b.points, p)
	return
}

func (b *flattenBuffer) Barrier(barrier edge.BarrierMessage) error {
	return edge.Forward(b.n.outs, barrier)
}
func (b *flattenBuffer) DeleteGroup(d edge.DeleteGroupMessage) error {
	return edge.Forward(b.n.outs, d)
}
func (b *flattenBuffer) Done() {}

func (n *FlattenNode) flatten(points []edge.FieldsTagsTimeGetter) (models.Fields, error) {
	fields := make(models.Fields)
	if len(points) == 0 {
		return fields, nil
	}
	fieldPrefix := n.bufPool.Get().(*bytes.Buffer)
	defer n.bufPool.Put(fieldPrefix)
POINTS:
	for _, p := range points {
		tags := p.Tags()
		for i, tag := range n.f.Dimensions {
			if v, ok := tags[tag]; ok {
				if i > 0 {
					fieldPrefix.WriteString(n.f.Delimiter)
				}
				fieldPrefix.WriteString(v)
			} else {
				n.diag.Error("point missing tag for flatten operation", fmt.Errorf("tag %s is missing from point", tag))
				continue POINTS
			}
		}
		l := fieldPrefix.Len()
		for fname, value := range p.Fields() {
			if !n.f.DropOriginalFieldNameFlag {
				if l > 0 {
					fieldPrefix.WriteString(n.f.Delimiter)
				}
				fieldPrefix.WriteString(fname)
			}
			fields[fieldPrefix.String()] = value
			fieldPrefix.Truncate(l)
		}
		fieldPrefix.Reset()
	}
	return fields, nil
}
