package kapacitor

import (
	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

func newTrickleNode(et *ExecutingTask, n *pipeline.TrickleNode, d NodeDiagnostic) *TrickleNode {
	sn := &TrickleNode{
		node: node{Node: n, et: et, diag: d},
	}
	sn.node.runF = sn.runTrickle
	return sn
}

type TrickleNode struct {
	node
	dims models.Dimensions
	name string
}

func (n *TrickleNode) runTrickle(_ []byte) error {
	consumer := edge.NewConsumerWithReceiver(
		n.ins[0],
		n,
	)
	return consumer.Consume()
}

// BeginBatch sets some batch variables on the node, and isn't forwarded.
func (n *TrickleNode) BeginBatch(b edge.BeginBatchMessage) error {
	n.dims = b.Dimensions()
	n.name = b.Name()
	return nil
}

// BatchPoint forwards a PointMessage
func (n *TrickleNode) BatchPoint(bp edge.BatchPointMessage) error {
	return n.outs[0].Collect(edge.NewPointMessage(
		n.name,
		"",
		"",
		n.dims,
		bp.Fields(),
		bp.Tags(),
		bp.Time()))
}

func (n *TrickleNode) EndBatch(end edge.EndBatchMessage) error {
	return nil
}

func (n *TrickleNode) Point(p edge.PointMessage) error {
	return n.outs[0].Collect(p)
}

func (n *TrickleNode) Barrier(barrier edge.BarrierMessage) error {
	return n.outs[0].Collect(barrier)
}

func (n *TrickleNode) DeleteGroup(d edge.DeleteGroupMessage) error {
	return n.outs[0].Collect(d)
}

func (n *TrickleNode) Done() {}
