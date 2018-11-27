package kapacitor

import (
	"errors"
	"time"

	"sync"
	"sync/atomic"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

type BarrierNode struct {
	node
	b              *pipeline.BarrierNode
	barrierStopper map[models.GroupID]func()
}

// Create a new  BarrierNode, which emits a barrier if data traffic has been idle for the configured amount of time.
func newBarrierNode(et *ExecutingTask, n *pipeline.BarrierNode, d NodeDiagnostic) (*BarrierNode, error) {
	if n.Idle == 0 && n.Period == 0 {
		return nil, errors.New("barrier node must have either a non zero idle or a non zero period")
	}
	bn := &BarrierNode{
		node:           node{Node: n, et: et, diag: d},
		b:              n,
		barrierStopper: map[models.GroupID]func(){},
	}
	bn.node.runF = bn.runBarrierEmitter
	return bn, nil
}

func (n *BarrierNode) runBarrierEmitter([]byte) error {
	defer n.stopBarrierEmitter()
	consumer := edge.NewGroupedConsumer(n.ins[0], n)
	n.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())
	return consumer.Consume()
}

func (n *BarrierNode) stopBarrierEmitter() {
	for _, stopF := range n.barrierStopper {
		stopF()
	}
}

func (n *BarrierNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	r, stopF, err := n.newBarrier(group, first)
	if err != nil {
		return nil, err
	}
	n.barrierStopper[group.ID] = stopF
	return edge.NewReceiverFromForwardReceiverWithStats(
		n.outs,
		edge.NewTimedForwardReceiver(n.timer, r),
	), nil
}

func (n *BarrierNode) newBarrier(group edge.GroupInfo, first edge.PointMeta) (edge.ForwardReceiver, func(), error) {
	switch {
	case n.b.Idle != 0:
		idleBarrier := newIdleBarrier(
			first.Name(),
			group,
			n.ins[0],
			n.b.Idle,
			n.outs,
			n.b.Delete,
		)
		return idleBarrier, idleBarrier.Stop, nil
	case n.b.Period != 0:
		periodicBarrier := newPeriodicBarrier(
			first.Name(),
			group,
			n.ins[0],
			n.b.Period,
			n.outs,
			n.b.Delete,
		)
		return periodicBarrier, periodicBarrier.Stop, nil
	default:
		return nil, nil, errors.New("unreachable code, barrier node should have non-zero idle or non-zero period")
	}
}

type idleBarrier struct {
	name  string
	group edge.GroupInfo
	in    edge.Edge

	del          bool
	idle         time.Duration
	lastPointT   atomic.Value
	lastBarrierT atomic.Value
	wg           sync.WaitGroup
	outs         []edge.StatsEdge
	stopC        chan struct{}
	resetTimerC  chan struct{}
}

func newIdleBarrier(name string, group edge.GroupInfo, in edge.Edge, idle time.Duration, outs []edge.StatsEdge, del bool) *idleBarrier {
	r := &idleBarrier{
		name:         name,
		group:        group,
		in:           in,
		idle:         idle,
		lastPointT:   atomic.Value{},
		lastBarrierT: atomic.Value{},
		wg:           sync.WaitGroup{},
		outs:         outs,
		stopC:        make(chan struct{}),
		resetTimerC:  make(chan struct{}),
		del:          del,
	}

	r.Init()

	return r
}

func (n *idleBarrier) Init() {
	n.lastPointT.Store(time.Now().UTC())
	n.lastBarrierT.Store(time.Time{})
	n.wg.Add(1)

	go n.idleHandler()
}

func (n *idleBarrier) Stop() {
	select {
	case <-n.stopC:
	default:
		close(n.stopC)
		n.wg.Wait()
	}
}

func (n *idleBarrier) BeginBatch(m edge.BeginBatchMessage) (edge.Message, error) {
	return m, nil
}
func (n *idleBarrier) BatchPoint(m edge.BatchPointMessage) (edge.Message, error) {
	if !m.Time().Before(n.lastBarrierT.Load().(time.Time)) {
		n.resetTimer()
		n.lastPointT.Store(m.Time())
		return m, nil
	}
	return nil, nil
}
func (n *idleBarrier) EndBatch(m edge.EndBatchMessage) (edge.Message, error) {
	return m, nil
}
func (n *idleBarrier) Barrier(m edge.BarrierMessage) (edge.Message, error) {
	if !m.Time().Before(n.lastBarrierT.Load().(time.Time)) {
		n.resetTimer()
		n.lastPointT.Store(m.Time())
		n.lastBarrierT.Store(m.Time())
		return m, nil
	}
	return nil, nil
}
func (n *idleBarrier) DeleteGroup(m edge.DeleteGroupMessage) (edge.Message, error) {
	if m.GroupID() == n.group.ID {
		n.Stop()
	}
	return m, nil
}
func (n *idleBarrier) Done() {}

func (n *idleBarrier) Point(m edge.PointMessage) (edge.Message, error) {
	if !m.Time().Before(n.lastBarrierT.Load().(time.Time)) {
		n.resetTimer()
		n.lastPointT.Store(m.Time())
		return m, nil
	}
	return nil, nil
}

func (n *idleBarrier) resetTimer() {
	n.resetTimerC <- struct{}{}
}

func (n *idleBarrier) emitBarrier() error {
	newT := n.lastPointT.Load().(time.Time).Add(n.idle)
	n.lastPointT.Store(newT)
	n.lastBarrierT.Store(newT)

	err := edge.Forward(n.outs, edge.NewBarrierMessage(n.group, newT))
	if err != nil {
		return err
	}
	if n.del {
		return n.in.Collect(edge.NewDeleteGroupMessage(n.group.ID))
	}
	return nil
}

func (n *idleBarrier) idleHandler() {
	defer n.wg.Done()
	idleTimer := time.NewTimer(n.idle)
	for {
		select {
		case <-n.resetTimerC:
			if !idleTimer.Stop() {
				<-idleTimer.C
			}
			idleTimer.Reset(n.idle)
		case <-idleTimer.C:
			n.emitBarrier()
			idleTimer.Reset(n.idle)
		case <-n.stopC:
			idleTimer.Stop()
			return
		}
	}
}

type periodicBarrier struct {
	name  string
	group edge.GroupInfo
	in    edge.Edge

	del    bool
	lastT  atomic.Value
	ticker *time.Ticker
	wg     sync.WaitGroup
	outs   []edge.StatsEdge
	stopC  chan struct{}
}

func newPeriodicBarrier(name string, group edge.GroupInfo, in edge.Edge, period time.Duration, outs []edge.StatsEdge, del bool) *periodicBarrier {
	r := &periodicBarrier{
		name:   name,
		group:  group,
		in:     in,
		lastT:  atomic.Value{},
		ticker: time.NewTicker(period),
		wg:     sync.WaitGroup{},
		outs:   outs,
		stopC:  make(chan struct{}),
		del:    del,
	}

	r.Init()

	return r
}

func (n *periodicBarrier) Init() {
	n.lastT.Store(time.Time{})
	n.wg.Add(1)

	go n.periodicEmitter()
}

func (n *periodicBarrier) Stop() {
	select {
	case <-n.stopC:
	default:
		close(n.stopC)
		n.ticker.Stop()
		n.wg.Wait()
	}
}

func (n *periodicBarrier) BeginBatch(m edge.BeginBatchMessage) (edge.Message, error) {
	return m, nil
}
func (n *periodicBarrier) BatchPoint(m edge.BatchPointMessage) (edge.Message, error) {
	if !m.Time().Before(n.lastT.Load().(time.Time)) {
		return m, nil
	}
	return nil, nil
}
func (n *periodicBarrier) EndBatch(m edge.EndBatchMessage) (edge.Message, error) {
	return m, nil
}
func (n *periodicBarrier) Barrier(m edge.BarrierMessage) (edge.Message, error) {
	if !m.Time().Before(n.lastT.Load().(time.Time)) {
		return m, nil
	}
	return nil, nil
}
func (n *periodicBarrier) DeleteGroup(m edge.DeleteGroupMessage) (edge.Message, error) {
	if m.GroupID() == n.group.ID {
		n.Stop()
	}
	return m, nil
}
func (n *periodicBarrier) Done() {}

func (n *periodicBarrier) Point(m edge.PointMessage) (edge.Message, error) {
	if !m.Time().Before(n.lastT.Load().(time.Time)) {
		return m, nil
	}
	return nil, nil
}

func (n *periodicBarrier) emitBarrier() error {
	nowT := time.Now().UTC()
	n.lastT.Store(nowT)
	err := edge.Forward(n.outs, edge.NewBarrierMessage(n.group, nowT))
	if err != nil {
		return err
	}
	if n.del {
		// Send DeleteGroupMessage into self
		return n.in.Collect(edge.NewDeleteGroupMessage(n.group.ID))
	}
	return nil
}

func (n *periodicBarrier) periodicEmitter() {
	defer n.wg.Done()
	for {
		select {
		case <-n.ticker.C:
			n.emitBarrier()
		case <-n.stopC:
			return
		}
	}
}
