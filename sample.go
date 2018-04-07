package kapacitor

import (
	"errors"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

type SampleNode struct {
	node
	s *pipeline.SampleNode

	counts   map[models.GroupID]int64
	duration time.Duration
}

// Create a new  SampleNode which filters data from a source.
func newSampleNode(et *ExecutingTask, n *pipeline.SampleNode, d NodeDiagnostic) (*SampleNode, error) {
	sn := &SampleNode{
		node:     node{Node: n, et: et, diag: d},
		s:        n,
		counts:   make(map[models.GroupID]int64),
		duration: n.Duration,
	}
	sn.node.runF = sn.runSample
	if n.Duration == 0 && n.N == 0 {
		return nil, errors.New("invalid sample rate: must be positive integer or duration")
	}
	return sn, nil
}

func (n *SampleNode) runSample([]byte) error {
	consumer := edge.NewGroupedConsumer(
		n.ins[0],
		n,
	)
	n.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())
	return consumer.Consume()
}

func (n *SampleNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	return edge.NewReceiverFromForwardReceiverWithStats(
		n.outs,
		edge.NewTimedForwardReceiver(n.timer, n.newGroup()),
	), nil
}
func (n *SampleNode) newGroup() *sampleGroup {
	return &sampleGroup{
		n: n,
	}
}

type sampleGroup struct {
	n *SampleNode

	count int64
}

func (g *sampleGroup) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	g.count = 0
	return begin, nil
}

func (g *sampleGroup) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	keep := g.n.shouldKeep(g.count, bp.Time())
	g.count++
	if keep {
		return bp, nil
	}
	return nil, nil
}

func (g *sampleGroup) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return end, nil
}

func (g *sampleGroup) Point(p edge.PointMessage) (edge.Message, error) {
	keep := g.n.shouldKeep(g.count, p.Time())
	g.count++
	if keep {
		return p, nil
	}
	return nil, nil
}

func (g *sampleGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (g *sampleGroup) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (g *sampleGroup) Done() {}

func (n *SampleNode) shouldKeep(count int64, t time.Time) bool {
	if n.duration != 0 {
		keepTime := t.Truncate(n.duration)
		return t.Equal(keepTime)
	} else {
		return count%n.s.N == 0
	}
}
