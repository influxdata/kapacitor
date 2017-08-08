package waiter

import (
	"errors"
	"sync"
)

// waiterBufferSize is the number broadcast events to buffer per waiter.
const waiterBufferSize = 100

// WaiterGroup provides a mechanism to notify multiple goroutines of the occurrence of events.
// WaiterGroup can be thought of as the inverse of sync.WaitGroup, instead of waiting for a group
// of goroutines to finish, WaiterGroup unblocks a group of waiting goroutines.
//
// Example:
//    g := NewGroup()
//    // Always call stop when it is no longer needed.
//    defer g.Stop()
//    w := g.NewWaiter()
//    go func() {
//        // Block until the broadcast.
//        for w.Wait() {
//            // ... do something now that event has occurred ...
//        }
//    }()
//    g.Broadcast()
//
type WaiterGroup struct {
	mu            sync.Mutex
	stopped       bool
	events        signalC
	newWaiters    chan *waiter
	removeWaiters chan *waiter
	stopping      signalC
	wg            sync.WaitGroup
}

// signal is an empty struct used to signal event.
var signal = struct{}{}

// signalC is a channel used to send signals only, no data will be sent over the channel.
// Signals are sent via closing the channel or sending an empty value.
type signalC chan struct{}

// NewGroup returns a new WaiterGroup, which must be stopped when it is no longer needed.
func NewGroup() *WaiterGroup {
	c := &WaiterGroup{
		events:        make(signalC),
		newWaiters:    make(chan *waiter),
		removeWaiters: make(chan *waiter),
		stopping:      make(signalC),
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.run()
	}()
	return c
}

// run is the internal event loop, that process requests to block and broadcasts the occurrence of the event.
func (g *WaiterGroup) run() {
	var waiting []*waiter
	for {
		select {
		case <-g.stopping:
			return
		case w := <-g.newWaiters:
			waiting = append(waiting, w)
		case remove := <-g.removeWaiters:
			filtered := waiting[0:0]
			for _, w := range waiting {
				if w != remove {
					filtered = append(filtered, w)
				} else {
					close(w.s)
				}
			}
			waiting = filtered
		case <-g.events:
			for _, w := range waiting {
				select {
				case w.s <- signal:
				case <-g.stopping:
					return
				default:
					// Drop broadcast event to waiter since it was not waiting.
				}
			}
		}
	}
}

// Broadcast signal that the event has occurred and unblocks all waiting goroutines.
func (g *WaiterGroup) Broadcast() {
	select {
	case g.events <- signal:
	case <-g.stopping:
	}
}

// Stop unblocks all waiting goroutines.
// Stop must be called when the group is no longer needed.
func (g *WaiterGroup) Stop() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return
	}
	g.stopped = true
	close(g.stopping)
	g.wg.Wait()
}

// NewWaiter creates and returns a new waiter for the group.
// An error is returned if the group is stopped.
//
// If NewWaiter is called concurrently with Broadcast there is no guarantee
// that all broadcast messages are sent to all Waiters.
func (g *WaiterGroup) NewWaiter() (Waiter, error) {
	w := &waiter{
		g: g,
		s: make(signalC, waiterBufferSize),
	}
	select {
	case g.newWaiters <- w:
		return w, nil
	case <-g.stopping:
		return nil, errors.New("group stopped")
	}
}

func (g *WaiterGroup) remove(w *waiter) {
	select {
	case g.removeWaiters <- w:
		// Read all events until the waiter signal channel is closed.
		for {
			select {
			case _, open := <-w.s:
				if !open {
					return
				}
			case <-g.stopping:
				return
			}
		}
	case <-g.stopping:
	}
}

// Waiter waits for a signal to be broadcast.
type Waiter interface {
	// Wait blocks until the signal is broadcast.
	// If the return value is false no more events will be broadcast.
	Wait() bool
	// Stop removes the waiter from the WaiterGroup.
	// Future calls to Wait will return false immediately.
	Stop()
}

type waiter struct {
	g *WaiterGroup
	// s is a channel on which broadcast signals are received.
	// Once the waiter is removed this channel will be closed.
	s signalC
}

func (w *waiter) Wait() bool {
	select {
	case _, open := <-w.s:
		return open
	case <-w.g.stopping:
		return false
	}
}

func (w *waiter) Stop() {
	w.g.remove(w)
}
