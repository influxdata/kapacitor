package edge

import "github.com/influxdata/kapacitor/timer"

type timedForwardReceiver struct {
	timer timer.Timer
	r     ForwardReceiver
}
type timedForwardBufferedReceiver struct {
	timedForwardReceiver
	b ForwardBufferedReceiver
}

// NewTimedForwardReceiver creates a forward receiver which times the time spent in r.
func NewTimedForwardReceiver(t timer.Timer, r ForwardReceiver) ForwardReceiver {
	b, ok := r.(ForwardBufferedReceiver)
	if ok {
		return &timedForwardBufferedReceiver{
			timedForwardReceiver: timedForwardReceiver{
				timer: t,
				r:     r,
			},
			b: b,
		}
	}
	return &timedForwardReceiver{
		timer: t,
		r:     r,
	}
}

func (tr *timedForwardReceiver) BeginBatch(begin BeginBatchMessage) (m Message, err error) {
	tr.timer.Start()
	m, err = tr.r.BeginBatch(begin)
	tr.timer.Stop()
	return
}

func (tr *timedForwardReceiver) BatchPoint(bp BatchPointMessage) (m Message, err error) {
	tr.timer.Start()
	m, err = tr.r.BatchPoint(bp)
	tr.timer.Stop()
	return
}

func (tr *timedForwardReceiver) EndBatch(end EndBatchMessage) (m Message, err error) {
	tr.timer.Start()
	m, err = tr.r.EndBatch(end)
	tr.timer.Stop()
	return
}

func (tr *timedForwardBufferedReceiver) BufferedBatch(batch BufferedBatchMessage) (m Message, err error) {
	tr.timer.Start()
	m, err = tr.b.BufferedBatch(batch)
	tr.timer.Stop()
	return
}

func (tr *timedForwardReceiver) Point(p PointMessage) (m Message, err error) {
	tr.timer.Start()
	m, err = tr.r.Point(p)
	tr.timer.Stop()
	return
}

func (tr *timedForwardReceiver) Barrier(b BarrierMessage) (m Message, err error) {
	tr.timer.Start()
	m, err = tr.r.Barrier(b)
	tr.timer.Stop()
	return
}
func (tr *timedForwardReceiver) DeleteGroup(d DeleteGroupMessage) (m Message, err error) {
	tr.timer.Start()
	m, err = tr.r.DeleteGroup(d)
	tr.timer.Stop()
	return
}

func (tr *timedForwardReceiver) Done() {
	tr.timer.Start()
	tr.r.Done()
	tr.timer.Stop()
}
