package kapacitor

import (
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

type WindowNode struct {
	node
	w *pipeline.WindowNode
}

// Create a new  WindowNode, which windows data for a period of time and emits the window.
func newWindowNode(et *ExecutingTask, n *pipeline.WindowNode, d NodeDiagnostic) (*WindowNode, error) {
	if n.Period == 0 && n.PeriodCount == 0 {
		return nil, errors.New("window node must have either a non zero period or non zero period count")
	}
	wn := &WindowNode{
		w:    n,
		node: node{Node: n, et: et, diag: d},
	}
	wn.node.runF = wn.runWindow
	return wn, nil
}

func (n *WindowNode) runWindow([]byte) (err error) {
	consumer := edge.NewGroupedConsumer(n.ins[0], n)
	n.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())
	err = consumer.Consume()
	return
}

func (n *WindowNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	r, err := n.newWindow(group, first)
	if err != nil {
		return nil, err
	}
	return edge.NewReceiverFromForwardReceiverWithStats(
		n.outs,
		edge.NewTimedForwardReceiver(n.timer, r),
	), nil
}

func (n *WindowNode) DeleteGroup(group models.GroupID) {
	// Nothing to do
}

func (n *WindowNode) newWindow(group edge.GroupInfo, first edge.PointMeta) (edge.ForwardReceiver, error) {
	switch {
	case n.w.Period != 0:
		return newWindowByTime(
			first.Name(),
			first.Time(),
			group,
			n.w.Period,
			n.w.Every,
			n.w.AlignFlag,
			n.w.FillPeriodFlag,
			n.diag,
		), nil
	case n.w.PeriodCount != 0:
		return newWindowByCount(
			first.Name(),
			group,
			int(n.w.PeriodCount),
			int(n.w.EveryCount),
			n.w.FillPeriodFlag,
			n.diag,
		), nil
	default:
		return nil, errors.New("unreachable code, window node should have a non-zero period or period count")
	}
}

type windowByTime struct {
	name  string
	group edge.GroupInfo

	nextEmit time.Time

	buf *windowTimeBuffer

	align,
	fillPeriod bool

	period time.Duration
	every  time.Duration

	diag NodeDiagnostic
}

func newWindowByTime(
	name string,
	t time.Time,
	group edge.GroupInfo,
	period,
	every time.Duration,
	align,
	fillPeriod bool,
	d NodeDiagnostic,

) *windowByTime {
	// Determine nextEmit time.
	var nextEmit time.Time
	if fillPeriod {
		nextEmit = t.Add(period)
		if align {
			firstPeriod := nextEmit
			// Needs to be aligned with Every and be greater than now+Period
			nextEmit = nextEmit.Truncate(every)
			if !nextEmit.After(firstPeriod) {
				// This means we will drop the first few points
				nextEmit = nextEmit.Add(every)
			}
		}
	} else {
		nextEmit = t.Add(every)
		if align {
			nextEmit = nextEmit.Truncate(every)
		}
	}
	return &windowByTime{
		name:       name,
		group:      group,
		nextEmit:   nextEmit,
		buf:        &windowTimeBuffer{diag: d},
		align:      align,
		fillPeriod: fillPeriod,
		period:     period,
		every:      every,
		diag:       d,
	}
}

func (w *windowByTime) BeginBatch(edge.BeginBatchMessage) (edge.Message, error) {
	return nil, errors.New("window does not support batch data")
}
func (w *windowByTime) BatchPoint(edge.BatchPointMessage) (edge.Message, error) {
	return nil, errors.New("window does not support batch data")
}
func (w *windowByTime) EndBatch(edge.EndBatchMessage) (edge.Message, error) {
	return nil, errors.New("window does not support batch data")
}
func (w *windowByTime) Barrier(b edge.BarrierMessage) (msg edge.Message, err error) {
	if w.every == 0 {
		// Since we are emitting every point we can use a right aligned window (oldest, now]
		if !b.Time().Before(w.nextEmit) {
			// purge old points
			oldest := b.Time().Add(-1 * w.period)
			w.buf.purge(oldest, false)

			// get current batch
			msg = w.batch(b.Time())

			// Next emit time is now
			w.nextEmit = b.Time()
		}
	} else {
		// Since more points can arrive with the same time we need to use a left aligned window [oldest, now).
		if !b.Time().Before(w.nextEmit) {
			// purge old points
			oldest := w.nextEmit.Add(-1 * w.period)
			w.buf.purge(oldest, true)

			// get current batch
			msg = w.batch(w.nextEmit)

			// Determine next emit time.
			// This is dependent on the current time not the last time we emitted.
			w.nextEmit = b.Time().Add(w.every)
			if w.align {
				w.nextEmit = w.nextEmit.Truncate(w.every)
			}
		}
	}
	return
}
func (w *windowByTime) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (w *windowByTime) Done() {}

func (w *windowByTime) Point(p edge.PointMessage) (msg edge.Message, err error) {
	if w.every == 0 {
		// Insert point before.
		w.buf.insert(p)
		// Since we are emitting every point we can use a right aligned window (oldest, now]
		if !p.Time().Before(w.nextEmit) {
			// purge old points
			oldest := p.Time().Add(-1 * w.period)
			w.buf.purge(oldest, false)

			// get current batch
			msg = w.batch(p.Time())

			// Next emit time is now
			w.nextEmit = p.Time()
		}
	} else {
		// Since more points can arrive with the same time we need to use a left aligned window [oldest, now).
		if !p.Time().Before(w.nextEmit) {
			// purge old points
			oldest := w.nextEmit.Add(-1 * w.period)
			w.buf.purge(oldest, true)

			// get current batch
			msg = w.batch(w.nextEmit)

			// Determine next emit time.
			// This is dependent on the current time not the last time we emitted.
			w.nextEmit = p.Time().Add(w.every)
			if w.align {
				w.nextEmit = w.nextEmit.Truncate(w.every)
			}
		}
		// Insert point after.
		w.buf.insert(p)
	}
	return
}

// batch returns the current window buffer as a batch message.
// TODO(nathanielc): A possible optimization could be to not buffer the data at all if we know that we do not have overlapping windows.
func (w *windowByTime) batch(tmax time.Time) edge.BufferedBatchMessage {
	points := w.buf.points()
	return edge.NewBufferedBatchMessage(
		edge.NewBeginBatchMessage(
			w.name,
			w.group.Tags,
			w.group.Dimensions.ByName,
			tmax,
			len(points),
		),
		points,
		edge.NewEndBatchMessage(),
	)
}

// implements a purpose built ring buffer for the window of points
type windowTimeBuffer struct {
	window []edge.PointMessage
	start  int
	stop   int
	size   int
	diag   NodeDiagnostic
}

// Insert a single point into the buffer.
func (b *windowTimeBuffer) insert(p edge.PointMessage) {
	if b.size == cap(b.window) {
		//Increase our buffer
		c := 2 * (b.size + 1)
		w := make([]edge.PointMessage, b.size+1, c)
		if b.size == 0 {
			//do nothing
		} else if b.stop > b.start {
			n := copy(w, b.window[b.start:b.stop])
			if n != b.size {
				panic(fmt.Sprintf("did not copy all the data: copied: %d size: %d start: %d stop: %d\n", n, b.size, b.start, b.stop))
			}
		} else {
			n := 0
			n += copy(w, b.window[b.start:])
			n += copy(w[b.size-b.start:], b.window[:b.stop])
			if n != b.size {
				panic(fmt.Sprintf("did not copy all the data: copied: %d size: %d start: %d stop: %d\n", n, b.size, b.start, b.stop))
			}
		}
		b.window = w
		b.start = 0
		b.stop = b.size
	}

	// Check if we need to wrap around
	if len(b.window) == cap(b.window) && b.stop == len(b.window) {
		b.stop = 0
	}

	// Insert point
	if b.stop == len(b.window) {
		b.window = append(b.window, p)
	} else {
		b.window[b.stop] = p
	}
	b.size++
	b.stop++
}

// Purge expired data from the window.
func (b *windowTimeBuffer) purge(oldest time.Time, inclusive bool) {
	include := func(t time.Time) bool {
		if inclusive {
			return !t.Before(oldest)
		}
		return t.After(oldest)
	}
	l := len(b.window)
	if l == 0 {
		return
	}
	if b.start < b.stop {
		for ; b.start < b.stop; b.start++ {
			if include(b.window[b.start].Time()) {
				break
			}
		}
		b.size = b.stop - b.start
	} else {
		if include(b.window[l-1].Time()) {
			for ; b.start < l; b.start++ {
				if include(b.window[b.start].Time()) {
					break
				}
			}
			b.size = l - b.start + b.stop
		} else {
			for b.start = 0; b.start < b.stop; b.start++ {
				if include(b.window[b.start].Time()) {
					break
				}
			}
			b.size = b.stop - b.start
		}
	}
}

// Returns a copy of the current buffer.
// TODO(nathanielc): Optimize this function use buffered vs unbuffered batch messages.
func (b *windowTimeBuffer) points() []edge.BatchPointMessage {
	if b.size == 0 {
		return nil
	}
	points := make([]edge.BatchPointMessage, b.size)
	if b.stop > b.start {
		for i, p := range b.window[b.start:b.stop] {
			points[i] = edge.BatchPointFromPoint(p)
		}
	} else {
		j := 0
		l := len(b.window)
		for i := b.start; i < l; i++ {
			p := b.window[i]
			points[j] = edge.BatchPointFromPoint(p)
			j++
		}
		for i := 0; i < b.stop; i++ {
			p := b.window[i]
			points[j] = edge.BatchPointFromPoint(p)
			j++
		}
	}
	return points
}

type windowByCount struct {
	name  string
	group edge.GroupInfo

	buf      []edge.BatchPointMessage
	start    int
	stop     int
	period   int
	every    int
	nextEmit int
	size     int
	count    int

	diag NodeDiagnostic
}

func newWindowByCount(
	name string,
	group edge.GroupInfo,
	period,
	every int,
	fillPeriod bool,
	d NodeDiagnostic,
) *windowByCount {
	// Determine the first nextEmit index
	nextEmit := every
	if fillPeriod {
		nextEmit = period
	}
	return &windowByCount{
		name:     name,
		group:    group,
		buf:      make([]edge.BatchPointMessage, period),
		period:   period,
		every:    every,
		nextEmit: nextEmit,
		diag:     d,
	}
}
func (w *windowByCount) BeginBatch(edge.BeginBatchMessage) (edge.Message, error) {
	return nil, errors.New("window does not support batch data")
}
func (w *windowByCount) BatchPoint(edge.BatchPointMessage) (edge.Message, error) {
	return nil, errors.New("window does not support batch data")
}
func (w *windowByCount) EndBatch(edge.EndBatchMessage) (edge.Message, error) {
	return nil, errors.New("window does not support batch data")
}
func (w *windowByCount) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	//TODO(nathanielc): Implement barrier messages to flush window
	return b, nil
}
func (w *windowByCount) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (w *windowByCount) Done() {}

func (w *windowByCount) Point(p edge.PointMessage) (msg edge.Message, err error) {
	w.buf[w.stop] = edge.BatchPointFromPoint(p)
	w.stop = (w.stop + 1) % w.period
	if w.size == w.period {
		w.start = (w.start + 1) % w.period
	} else {
		w.size++
	}
	w.count++
	//Check if its time to emit
	if w.count == w.nextEmit {
		w.nextEmit += w.every
		msg = w.batch()
	}
	return
}

func (w *windowByCount) batch() edge.BufferedBatchMessage {
	points := w.points()
	return edge.NewBufferedBatchMessage(
		edge.NewBeginBatchMessage(
			w.name,
			w.group.Tags,
			w.group.Dimensions.ByName,
			points[len(points)-1].Time(),
			len(points),
		),
		points,
		edge.NewEndBatchMessage(),
	)
}

// Returns a copy of the current buffer.
func (w *windowByCount) points() []edge.BatchPointMessage {
	if w.size == 0 {
		return nil
	}
	points := make([]edge.BatchPointMessage, w.size)
	if w.stop > w.start {
		copy(points, w.buf[w.start:w.stop])
	} else {
		j := 0
		l := len(w.buf)
		for i := w.start; i < l; i++ {
			points[j] = w.buf[i]
			j++
		}
		for i := 0; i < w.stop; i++ {
			points[j] = w.buf[i]
			j++
		}
	}
	return points
}
