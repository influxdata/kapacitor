package kapacitor

import (
	"fmt"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

const (
	statsKapacitorLoopbackPointsWritten = "points_written"
)

type KapacitorLoopbackNode struct {
	node
	k *pipeline.KapacitorLoopbackNode

	pointsWritten *expvar.Int

	begin edge.BeginBatchMessage
}

func newKapacitorLoopbackNode(et *ExecutingTask, n *pipeline.KapacitorLoopbackNode, d NodeDiagnostic) (*KapacitorLoopbackNode, error) {
	kn := &KapacitorLoopbackNode{
		node: node{Node: n, et: et, diag: d},
		k:    n,
	}
	kn.node.runF = kn.runOut
	// Check that a loop has not been created within this task
	for _, dbrp := range et.Task.DBRPs {
		if dbrp.Database == n.Database && dbrp.RetentionPolicy == n.RetentionPolicy {
			return nil, fmt.Errorf("loop detected on dbrp: %v", dbrp)
		}
	}
	return kn, nil
}

func (n *KapacitorLoopbackNode) runOut([]byte) error {
	n.pointsWritten = &expvar.Int{}
	n.statMap.Set(statsInfluxDBPointsWritten, n.pointsWritten)

	consumer := edge.NewConsumerWithReceiver(
		n.ins[0],
		n,
	)
	return consumer.Consume()
}

func (n *KapacitorLoopbackNode) Point(p edge.PointMessage) error {
	n.timer.Start()
	defer n.timer.Stop()

	p = p.ShallowCopy()

	if n.k.Database != "" {
		p.SetDatabase(n.k.Database)
	}
	if n.k.RetentionPolicy != "" {
		p.SetRetentionPolicy(n.k.RetentionPolicy)
	}
	if n.k.Measurement != "" {
		p.SetName(n.k.Measurement)
	}
	if len(n.k.Tags) > 0 {
		tags := p.Tags().Copy()
		for k, v := range n.k.Tags {
			tags[k] = v
		}
		p.SetTags(tags)
	}

	n.timer.Pause()
	err := n.et.tm.WriteKapacitorPoint(p)
	n.timer.Resume()

	if err != nil {
		n.diag.Error("failed to write point over loopback", err)

	} else {
		n.pointsWritten.Add(1)
	}
	return nil
}

func (n *KapacitorLoopbackNode) BeginBatch(begin edge.BeginBatchMessage) error {
	n.begin = begin
	return nil
}

func (n *KapacitorLoopbackNode) BatchPoint(bp edge.BatchPointMessage) error {
	tags := bp.Tags()
	if len(n.k.Tags) > 0 {
		tags = bp.Tags().Copy()
		for k, v := range n.k.Tags {
			tags[k] = v
		}
	}
	p := edge.NewPointMessage(
		n.begin.Name(),
		n.k.Database,
		n.k.RetentionPolicy,
		models.Dimensions{},
		bp.Fields(),
		tags,
		bp.Time(),
	)

	n.timer.Pause()
	err := n.et.tm.WriteKapacitorPoint(p)
	n.timer.Resume()

	if err != nil {
		n.diag.Error("failed to write point over loopback", err)
	} else {
		n.pointsWritten.Add(1)
	}
	return nil
}
func (n *KapacitorLoopbackNode) EndBatch(edge.EndBatchMessage) error {
	return nil
}
func (n *KapacitorLoopbackNode) Barrier(edge.BarrierMessage) error {
	return nil
}
func (n *KapacitorLoopbackNode) DeleteGroup(edge.DeleteGroupMessage) error {
	return nil
}
func (n *KapacitorLoopbackNode) Done() {}
