package kapacitor

import (
	"sync"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/pipeline"
)

type UnionNode struct {
	node
	u *pipeline.UnionNode

	// Buffer of points/batches from each source.
	sources []*timeMessageCircularQueue
	// the low water marks for each source.
	lowMarks []time.Time
	lock     sync.Mutex
	rename   string
}

//go:generate tmpl -data "[\"timeMessage\"]" -o=union_circularqueues.gen.go circularqueue.gen.go.tmpl

type timeMessage interface {
	edge.Message
	edge.TimeGetter
}

// Create a new  UnionNode which combines all parent data streams into a single stream.
// No transformation of any kind is performed.
func newUnionNode(et *ExecutingTask, n *pipeline.UnionNode, d NodeDiagnostic) (*UnionNode, error) {
	un := &UnionNode{
		u:      n,
		node:   node{Node: n, et: et, diag: d},
		rename: n.Rename,
	}
	un.node.runF = un.runUnion
	return un, nil
}

func (n *UnionNode) runUnion([]byte) error {
	// Keep buffer of values from parents so they can be ordered.

	n.sources = make([]*timeMessageCircularQueue, len(n.ins))
	for i := range n.ins {
		n.sources[i] = newTimeMessageCircularQueue()
	}
	n.lowMarks = make([]time.Time, len(n.ins))

	consumer := edge.NewMultiConsumerWithStats(n.ins, n)
	return consumer.Consume()
}

func (n *UnionNode) BufferedBatch(src int, batch edge.BufferedBatchMessage) error {
	n.timer.Start()
	defer n.timer.Stop()

	if n.rename != "" {
		batch = batch.ShallowCopy()
		batch.SetBegin(batch.Begin().ShallowCopy())
		batch.Begin().SetName(n.rename)
	}

	// Add newest point to buffer
	n.sources[src].Enqueue(batch)

	// Emit the next values
	return n.emitReady(false)
}

func (n *UnionNode) Delete(src int, d edge.DeleteGroupMessage) error {
	return edge.Forward(n.outs, d)
}

func (n *UnionNode) Point(src int, p edge.PointMessage) error {
	n.timer.Start()
	defer n.timer.Stop()

	if n.rename != "" {
		p = p.ShallowCopy()
		p.SetName(n.rename)
	}

	// Add newest point to buffer
	n.sources[src].Enqueue(p)

	// Emit the next values
	return n.emitReady(false)
}

func (n *UnionNode) Barrier(src int, b edge.BarrierMessage) error {
	n.timer.Start()
	defer n.timer.Stop()

	// Add newest point to buffer
	n.sources[src].Enqueue(b)

	// Emit the next values
	return n.emitReady(false)
}

func (n *UnionNode) Finish() error {
	// We are done, emit all buffered
	return n.emitReady(true)
}

func (n *UnionNode) emitReady(drain bool) error {
	emitted := true
	var v timeMessage
	var i int
	// Emit all points until nothing changes
	for emitted {
		emitted = false
		// Find low water mark
		var mark time.Time
		validSources := 0
		for i, values := range n.sources {
			sourceMark := n.lowMarks[i]
			if values.Len() > 0 {
				t := values.Peek(0).Time()
				if mark.IsZero() || t.Before(mark) {
					mark = t
				}
				sourceMark = t
			}
			n.lowMarks[i] = sourceMark
			if !sourceMark.IsZero() {
				validSources++
				// Only consider the sourceMark if we are not draining
				if !drain && (mark.IsZero() || sourceMark.Before(mark)) {
					mark = sourceMark
				}
			}
		}
		if !drain && validSources != len(n.sources) {
			// We can't continue processing until we have
			// at least one value from each parent.
			// Unless we are draining the buffer than we can continue.
			return nil
		}

		// Emit all values that are at or below the mark.
		for i = range n.sources {
			l := n.sources[i].Len()
			j := 0
			for j = 0; j < l; j++ {
				v = n.sources[i].Peek(j)
				if !v.Time().After(mark) {
					err := n.emit(v)
					if err != nil {
						return err
					}
					// Note that we emitted something
					emitted = true
				} else {
					break
				}
			}
			n.sources[i].Dequeue(j)
		}
	}
	return nil
}

func (n *UnionNode) emit(m edge.Message) error {
	n.timer.Pause()
	defer n.timer.Resume()
	return edge.Forward(n.outs, m)
}
