package kapacitor

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

type WindowNode struct {
	node
	w *pipeline.WindowNode
}

type window struct {
	buf      *windowBuffer
	nextEmit time.Time
	period   time.Duration
	every    time.Duration
}

// Create a new  WindowNode, which windows data for a period of time and emits the window.
func newWindowNode(et *ExecutingTask, n *pipeline.WindowNode) (*WindowNode, error) {
	wn := &WindowNode{
		w:    n,
		node: node{Node: n, et: et},
	}
	wn.node.runF = wn.runWindow
	return wn, nil
}

func (w *WindowNode) runWindow() error {

	windows := make(map[models.GroupID]*window)
	// Loops through points windowing by group
	for p := w.ins[0].NextPoint(); p != nil; p = w.ins[0].NextPoint() {
		wnd := windows[p.Group]
		if wnd == nil {
			wnd = &window{
				buf:      &windowBuffer{logger: w.logger},
				nextEmit: p.Time.Add(w.w.Every),
				period:   w.w.Period,
				every:    w.w.Every,
			}
			windows[p.Group] = wnd
		}
		if !p.Time.Before(wnd.nextEmit) {
			points := wnd.emit(p.Time)
			// Send window to all children
			for _, child := range w.outs {
				child.CollectBatch(points)
			}
		}
		wnd.buf.insert(p)
	}
	return nil
}

func (w *window) emit(now time.Time) []*models.Point {
	oldest := now.Add(-1 * w.period)
	w.buf.purge(oldest)

	w.nextEmit = w.nextEmit.Add(w.every)

	points := w.buf.points()

	return points
}

// implements a purpose built ring buffer for the window of points
type windowBuffer struct {
	sync.Mutex
	window []*models.Point
	start  int
	stop   int
	size   int
	logger *log.Logger
}

// Insert a single point into the buffer.
func (b *windowBuffer) insert(p *models.Point) {
	if p == nil {
		panic("do not insert nil values")
	}
	b.Lock()
	defer b.Unlock()
	if b.size == cap(b.window) {
		//Increase our buffer
		c := 2 * (b.size + 1)
		w := make([]*models.Point, b.size+1, c)
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
		if b.window[l-1].Time.After(oldest) {
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
func (b *windowBuffer) points() []*models.Point {
	b.Lock()
	defer b.Unlock()
	buf := make([]*models.Point, b.size)
	if b.size == 0 {
		return buf
	}
	if b.stop > b.start {
		for i, p := range b.window[b.start:b.stop] {
			buf[i] = p
		}
	} else {
		j := 0
		l := len(b.window)
		for i := b.start; i < l; i++ {
			buf[j] = b.window[i]
			j++
		}
		for i := 0; i < b.stop; i++ {
			p := b.window[i]
			buf[j] = p
			j++
		}
	}
	return buf
}
