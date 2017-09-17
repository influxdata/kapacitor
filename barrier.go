package kapacitor

import (
	"errors"
	"time"

	"sync"
	"sync/atomic"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/pipeline"
)

type BarrierNode struct {
	node
	b *pipeline.BarrierNode

	idle   time.Duration
	period time.Duration
	lastT  atomic.Value
	timer  *time.Timer
	ticker *time.Ticker
	wg     sync.WaitGroup
}

// Create a new  BarrierNode, which emits a barrier if data traffic has been idle for the configured amount of time.
func newBarrierNode(et *ExecutingTask, n *pipeline.BarrierNode, d NodeDiagnostic) (*BarrierNode, error) {
	if n.Idle == 0 && n.Period == 0 {
		return nil, errors.New("barrier node must have either a non zero idle or a non zero period")
	}
	bn := &BarrierNode{
		b:      n,
		node:   node{Node: n, et: et, diag: d},
		idle:   n.Idle,
		period: n.Period,
	}
	bn.node.runF = bn.runBarrierEmitter
	return bn, nil
}

func (n *BarrierNode) runBarrierEmitter([]byte) error {
	n.lastT.Store(time.Time{})
	stopC := make(chan bool, 1)
	n.wg.Add(1)

	if n.idle > 0 {
		n.timer = time.NewTimer(n.idle)
		defer func() {
			stopC <- true
			n.timer.Stop()
			n.wg.Wait()
		}()
		go n.idleHandler(stopC)
	}
	if n.period > 0 {
		n.ticker = time.NewTicker(n.period)
		defer func() {
			stopC <- true
			n.ticker.Stop()
			n.wg.Wait()
		}()
		go n.periodicEmitter(stopC)
	}

	consumer := edge.NewConsumerWithReceiver(n.ins[0], n)
	return consumer.Consume()
}

func (n *BarrierNode) BeginBatch(m edge.BeginBatchMessage) error {
	return edge.Forward(n.outs, m)
}
func (n *BarrierNode) BatchPoint(m edge.BatchPointMessage) error {
	if !m.Time().Before(n.lastT.Load().(time.Time)) {
		n.resetTimer()
		return edge.Forward(n.outs, m)
	}
	return nil
}
func (n *BarrierNode) EndBatch(m edge.EndBatchMessage) error {
	return edge.Forward(n.outs, m)
}
func (n *BarrierNode) Barrier(m edge.BarrierMessage) error {
	if !m.Time().Before(n.lastT.Load().(time.Time)) {
		n.resetTimer()
		return edge.Forward(n.outs, m)
	}
	return nil
}
func (n *BarrierNode) DeleteGroup(m edge.DeleteGroupMessage) error {
	return edge.Forward(n.outs, m)
}

func (n *BarrierNode) Point(m edge.PointMessage) error {
	if !m.Time().Before(n.lastT.Load().(time.Time)) {
		n.resetTimer()
		return edge.Forward(n.outs, m)
	}
	return nil
}

func (n *BarrierNode) resetTimer() {
	if n.idle > 0 {
		n.timer.Reset(n.idle)
	}
}

func (n *BarrierNode) emitBarrier() error {
	nowT := time.Now()
	n.lastT.Store(nowT)
	return edge.Forward(n.outs, edge.NewBarrierMessage(nowT))
}

func (n *BarrierNode) idleHandler(stopC <-chan bool) {
	defer n.wg.Done()
	for {
		select {
		case <-n.timer.C:
			n.emitBarrier()
			n.resetTimer()
		case <-stopC:
			return
		}
	}
}

func (n *BarrierNode) periodicEmitter(stopC <-chan bool) {
	defer n.wg.Done()
	for {
		select {
		case <-n.ticker.C:
			n.emitBarrier()
		case <-stopC:
			return
		}
	}
}
