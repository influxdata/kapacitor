package kapacitor

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

type WindowNode struct {
	node
	w *pipeline.WindowNode
}

// Create a new  WindowNode, which windows data for a period of time and emits the window.
func newWindowNode(et *ExecutingTask, n *pipeline.WindowNode, l *log.Logger) (*WindowNode, error) {
	wn := &WindowNode{
		w:    n,
		node: node{Node: n, et: et, logger: l},
	}
	wn.node.runF = wn.runWindow
	return wn, nil
}

type window interface {
	Insert(p models.Point) (models.Batch, bool)
}

func (w *WindowNode) runWindow([]byte) error {
	windows := make(map[models.GroupID]window)
	// Loops through points windowing by group
	for p, ok := w.ins[0].NextPoint(); ok; p, ok = w.ins[0].NextPoint() {
		w.timer.Start()
		wnd := windows[p.Group]
		if wnd == nil {
			tags := make(map[string]string, len(p.Dimensions.TagNames))
			for _, dim := range p.Dimensions.TagNames {
				tags[dim] = p.Tags[dim]
			}
			switch {
			case w.w.Period != 0:
				// Window by time
				wnd = newWindowByTime(
					p.Time,
					w.w.Period,
					w.w.Every,
					p.Name,
					p.Group,
					w.w.AlignFlag,
					p.Dimensions.ByName,
					w.w.FillPeriodFlag,
					tags,
					w.logger,
				)
			case w.w.PeriodCount != 0:
				wnd = newWindowByCount(
					p.Name,
					p.Group,
					tags,
					p.Dimensions.ByName,
					int(w.w.PeriodCount),
					int(w.w.EveryCount),
					w.w.FillPeriodFlag,
					w.logger,
				)
			default:
				// This should not be possible, but just in case.
				return errors.New("invalid window, no period specified for either time or count")
			}
			windows[p.Group] = wnd
		}
		batch, ok := wnd.Insert(p)
		if ok {
			// Send window to all children
			w.timer.Pause()
			for _, child := range w.outs {
				err := child.CollectBatch(batch)
				if err != nil {
					return err
				}
			}
			w.timer.Resume()
		}
		w.timer.Stop()
	}
	return nil
}

type windowByTime struct {
	buf      *windowTimeBuffer
	align    bool
	nextEmit time.Time
	period   time.Duration
	every    time.Duration
	name     string
	group    models.GroupID
	byName   bool
	tags     map[string]string
	logger   *log.Logger
}

func newWindowByTime(
	now time.Time,
	period,
	every time.Duration,
	name string,
	group models.GroupID,
	align,
	byName,
	fillPeriod bool,
	tags models.Tags,
	logger *log.Logger,

) *windowByTime {
	// Determine first nextEmit time.
	var nextEmit time.Time
	if fillPeriod {
		nextEmit = now.Add(period)
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
		nextEmit = now.Add(every)
		if align {
			nextEmit = nextEmit.Truncate(every)
		}
	}
	return &windowByTime{
		buf:      &windowTimeBuffer{logger: logger},
		nextEmit: nextEmit,
		align:    align,
		period:   period,
		every:    every,
		name:     name,
		group:    group,
		byName:   byName,
		tags:     tags,
		logger:   logger,
	}
}

func (w *windowByTime) Insert(p models.Point) (b models.Batch, ok bool) {
	if w.every == 0 {
		// Insert point before.
		w.buf.insert(p)
		// Since we are emitting every point we can use a right aligned window (oldest, now]
		if !p.Time.Before(w.nextEmit) {
			// purge old points
			oldest := p.Time.Add(-1 * w.period)
			w.buf.purge(oldest, false)

			// get current batch
			b = w.batch(p.Time)
			ok = true

			// Next emit time is now
			w.nextEmit = p.Time
		}
	} else {
		// Since more points can arrive with the same time we need to use a left aligned window [oldest, now).
		if !p.Time.Before(w.nextEmit) {
			// purge old points
			oldest := w.nextEmit.Add(-1 * w.period)
			w.buf.purge(oldest, true)

			// get current batch
			b = w.batch(w.nextEmit)
			ok = true

			// Determine next emit time.
			// This is dependent on the current time not the last time we emitted.
			w.nextEmit = p.Time.Add(w.every)
			if w.align {
				w.nextEmit = w.nextEmit.Truncate(w.every)
			}
		}
		// Insert point after.
		w.buf.insert(p)
	}
	return
}

func (w *windowByTime) batch(tmax time.Time) models.Batch {
	return models.Batch{
		Name:   w.name,
		Group:  w.group,
		Tags:   w.tags,
		TMax:   tmax,
		ByName: w.byName,
		Points: w.buf.points(),
	}
}

// implements a purpose built ring buffer for the window of points
type windowTimeBuffer struct {
	window []models.Point
	start  int
	stop   int
	size   int
	logger *log.Logger
}

// Insert a single point into the buffer.
func (b *windowTimeBuffer) insert(p models.Point) {
	if b.size == cap(b.window) {
		//Increase our buffer
		c := 2 * (b.size + 1)
		w := make([]models.Point, b.size+1, c)
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
			if include(b.window[b.start].Time) {
				break
			}
		}
		b.size = b.stop - b.start
	} else {
		if include(b.window[l-1].Time) {
			for ; b.start < l; b.start++ {
				if include(b.window[b.start].Time) {
					break
				}
			}
			b.size = l - b.start + b.stop
		} else {
			for b.start = 0; b.start < b.stop; b.start++ {
				if include(b.window[b.start].Time) {
					break
				}
			}
			b.size = b.stop - b.start
		}
	}
}

// Returns a copy of the current buffer.
func (b *windowTimeBuffer) points() []models.BatchPoint {
	if b.size == 0 {
		return nil
	}
	points := make([]models.BatchPoint, b.size)
	if b.stop > b.start {
		for i, p := range b.window[b.start:b.stop] {
			points[i] = models.BatchPointFromPoint(p)
		}
	} else {
		j := 0
		l := len(b.window)
		for i := b.start; i < l; i++ {
			p := b.window[i]
			points[j] = models.BatchPointFromPoint(p)
			j++
		}
		for i := 0; i < b.stop; i++ {
			p := b.window[i]
			points[j] = models.BatchPointFromPoint(p)
			j++
		}
	}
	return points
}

type windowByCount struct {
	name   string
	group  models.GroupID
	tags   models.Tags
	byName bool

	buf      []models.BatchPoint
	start    int
	stop     int
	period   int
	every    int
	nextEmit int
	size     int
	count    int

	logger *log.Logger
}

func newWindowByCount(
	name string,
	group models.GroupID,
	tags models.Tags,
	byName bool,
	period,
	every int,
	fillPeriod bool,
	logger *log.Logger) *windowByCount {
	// Determine the first nextEmit index
	nextEmit := every
	if fillPeriod {
		nextEmit = period
	}
	return &windowByCount{
		name:     name,
		group:    group,
		tags:     tags,
		byName:   byName,
		buf:      make([]models.BatchPoint, period),
		period:   period,
		every:    every,
		nextEmit: nextEmit,
		logger:   logger,
	}
}

func (w *windowByCount) Insert(p models.Point) (b models.Batch, ok bool) {
	w.buf[w.stop] = models.BatchPoint{
		Time:   p.Time,
		Fields: p.Fields,
		Tags:   p.Tags,
	}
	w.stop = (w.stop + 1) % w.period
	if w.size == w.period {
		w.start = (w.start + 1) % w.period
	} else {
		w.size++
	}
	w.count++
	//Check if its time to emit
	if w.count == w.nextEmit {
		b = w.batch()
		ok = true
	}
	return
}

func (w *windowByCount) batch() models.Batch {
	w.nextEmit += w.every
	points := w.points()
	return models.Batch{
		Name:   w.name,
		Group:  w.group,
		Tags:   w.tags,
		TMax:   points[len(points)-1].Time,
		ByName: w.byName,
		Points: points,
	}
}

// Returns a copy of the current buffer.
func (w *windowByCount) points() []models.BatchPoint {
	if w.size == 0 {
		return nil
	}
	points := make([]models.BatchPoint, w.size)
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
