/*
A clock that provides blocking calls that wait until absolute times have occurred. The clock can be controlled programmatically or be based of real time.
*/
package clock

import (
	"sync"
	"time"
)

// A clock interface to read time and wait until an absolute time arrives.
// Three implementations are available: A 'wall' clock that is based on realtime, a 'fast' clock that is always ahead and a 'set' clock that can be controlled via a setting time explicitly.
type Clock interface {
	Setter
	// Wait until time t has arrived. If t is in the past it immediately returns.
	Until(t time.Time)
}

type Setter interface {
	// Returns the start or 'zero' time of the clock
	Zero() time.Time
	// Set the time on the clock
	Set(t time.Time)
}

// realtime implementation of clock
type wallclock struct {
	zero time.Time
}

// Get a realtime wall clock.
func Wall() Clock {
	return &wallclock{time.Now()}
}

func (w *wallclock) Zero() time.Time {
	return w.zero
}

func (w *wallclock) Until(t time.Time) {
	time.Sleep(t.Sub(time.Now()))
}

func (w *wallclock) Set(t time.Time) {}

// implementation of clock that is always in the future
type fastclock struct {
	zero time.Time
}

// Get a realtime fast clock.
func Fast() Clock {
	return &fastclock{time.Now()}
}

func (f *fastclock) Zero() time.Time {
	return f.zero
}

func (f *fastclock) Until(t time.Time) {}

func (f *fastclock) Set(t time.Time) {}

// setable implementation of the clock
type setclock struct {
	zero time.Time
	now  time.Time
	cond *sync.Cond
}

// Get a clock that can be controlled programmatically.
func New(start time.Time) Clock {
	l := &sync.Mutex{}
	c := &setclock{
		zero: start,
		now:  start,
		cond: sync.NewCond(l),
	}
	return c
}

func (c *setclock) Zero() time.Time {
	return c.zero
}

func (c *setclock) Until(t time.Time) {
	c.cond.L.Lock()
	for t.After(c.now) {
		c.cond.Wait()
	}
	c.cond.L.Unlock()
}

func (c *setclock) Set(t time.Time) {
	if t.Before(c.now) {
		panic("cannot set time backwards")
	}
	c.cond.L.Lock()
	c.now = t
	c.cond.Broadcast()
	c.cond.L.Unlock()
}
