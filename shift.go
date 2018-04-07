package kapacitor

import (
	"errors"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/pipeline"
)

type ShiftNode struct {
	node
	s *pipeline.ShiftNode

	shift time.Duration
}

// Create a new  ShiftNode which shifts points and batches in time.
func newShiftNode(et *ExecutingTask, n *pipeline.ShiftNode, d NodeDiagnostic) (*ShiftNode, error) {
	sn := &ShiftNode{
		node:  node{Node: n, et: et, diag: d},
		s:     n,
		shift: n.Shift,
	}
	sn.node.runF = sn.runShift
	if n.Shift == 0 {
		return nil, errors.New("invalid shift value: must be non zero duration")
	}
	return sn, nil
}

func (n *ShiftNode) runShift([]byte) error {
	consumer := edge.NewConsumerWithReceiver(
		n.ins[0],
		edge.NewReceiverFromForwardReceiverWithStats(
			n.outs,
			edge.NewTimedForwardReceiver(n.timer, n),
		),
	)
	return consumer.Consume()
}

func (n *ShiftNode) doShift(t edge.TimeSetter) {
	t.SetTime(t.Time().Add(n.shift))
}

func (n *ShiftNode) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	begin = begin.ShallowCopy()
	n.doShift(begin)
	return begin, nil
}

func (n *ShiftNode) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	bp = bp.ShallowCopy()
	n.doShift(bp)
	return bp, nil
}

func (n *ShiftNode) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return end, nil
}

func (n *ShiftNode) Point(p edge.PointMessage) (edge.Message, error) {
	p = p.ShallowCopy()
	n.doShift(p)
	return p, nil
}

func (n *ShiftNode) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (n *ShiftNode) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (n *ShiftNode) Done() {}
