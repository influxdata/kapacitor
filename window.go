package kapacitor

import (
	"fmt"
	"log"
	"sync"
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

func (w *WindowNode) runWindow([]byte) error {
	windows := make(map[models.GroupID]*window)
	// Loops through points windowing by group
	for p, ok := w.ins[0].NextPoint(); ok; p, ok = w.ins[0].NextPoint() {
		w.timer.Start()
		wnd := windows[p.Group]
		if wnd == nil {
			tags := make(map[string]string, len(p.Dimensions))
			for _, dim := range p.Dimensions {
				tags[dim] = p.Tags[dim]
			}
			nextEmit := p.Time.Add(w.w.Every)
			if w.w.AlignFlag {
				nextEmit = nextEmit.Truncate(w.w.Every)
			}
			wnd = &window{
				buf:      &windowBuffer{logger: w.logger},
				align:    w.w.AlignFlag,
				nextEmit: nextEmit,
				period:   w.w.Period,
				every:    w.w.Every,
				name:     p.Name,
				group:    p.Group,
				tags:     tags,
				logger:   w.logger,
			}
			windows[p.Group] = wnd
		}
		if !p.Time.Before(wnd.nextEmit) {
			points := wnd.emit(p.Time)
			// Send window to all children
			w.timer.Pause()
			for _, child := range w.outs {
				child.CollectBatch(points)
			}
			w.timer.Resume()
		}
		wnd.buf.insert(p)
		w.timer.Stop()
	}
	return nil
}

type window struct {
	buf      *windowBuffer
	align    bool
	nextEmit time.Time
	period   time.Duration
	every    time.Duration
	name     string
	group    models.GroupID
	tags     map[string]string
	logger   *log.Logger
}

func (w *window) emit(now time.Time) models.Batch {
	oldest := w.nextEmit.Add(-1 * w.period)
	w.buf.purge(oldest)

	batch := w.buf.batch()
	batch.Name = w.name
	batch.Group = w.group
	batch.Tags = w.tags
	batch.TMax = w.nextEmit

	// Determine next emit time.
	// This is dependent on the current time not the last time we emitted.
	w.nextEmit = now.Add(w.every)
	if w.align {
		w.nextEmit = w.nextEmit.Truncate(w.every)
	}
	return batch
}

// implements a purpose built ring buffer for the window of points
type windowBuffer struct {
	sync.Mutex
	window []models.Point
	start  int
	stop   int
	size   int
	logger *log.Logger
}

// Insert a single point into the buffer.
func (b *windowBuffer) insert(p models.Point) {
	b.Lock()
	defer b.Unlock()
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
func (b *windowBuffer) purge(oldest time.Time) {
	b.Lock()
	defer b.Unlock()
	l := len(b.window)
	if l == 0 {
		return
	}
	if b.start < b.stop {
		for ; b.start < b.stop; b.start++ {
			if !b.window[b.start].Time.Before(oldest) {
				break
			}
		}
		b.size = b.stop - b.start
	} else {
		if !b.window[l-1].Time.Before(oldest) {
			for ; b.start < l; b.start++ {
				if !b.window[b.start].Time.Before(oldest) {
					break
				}
			}
			b.size = l - b.start + b.stop
		} else {
			for b.start = 0; b.start < b.stop; b.start++ {
				if !b.window[b.start].Time.Before(oldest) {
					break
				}
			}
			b.size = b.stop - b.start
		}
	}
}

// Returns a copy of the current buffer.
func (b *windowBuffer) batch() models.Batch {
	b.Lock()
	defer b.Unlock()
	batch := models.Batch{}
	if b.size == 0 {
		return batch
	}
	batch.Points = make([]models.BatchPoint, b.size)
	if b.stop > b.start {
		for i, p := range b.window[b.start:b.stop] {
			batch.Points[i] = models.BatchPointFromPoint(p)
		}
	} else {
		j := 0
		l := len(b.window)
		for i := b.start; i < l; i++ {
			p := b.window[i]
			batch.Points[j] = models.BatchPointFromPoint(p)
			j++
		}
		for i := 0; i < b.stop; i++ {
			p := b.window[i]
			batch.Points[j] = models.BatchPointFromPoint(p)
			j++
		}
	}
	return batch
}
