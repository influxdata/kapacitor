package waiter_test

import (
	"testing"
	"time"

	"github.com/influxdata/kapacitor/waiter"
)

func TestWaiterGroup_Wait_Broadcast(t *testing.T) {
	t.Parallel()
	g := waiter.NewGroup()
	defer g.Stop()

	count := 10
	waiters := make([]waiter.Waiter, count)
	for i := range waiters {
		var err error
		waiters[i], err = g.NewWaiter()
		if err != nil {
			t.Fatal(err)
		}
	}

	finished := make(chan bool, count)
	for _, w := range waiters {
		go func(w waiter.Waiter) {
			finished <- w.Wait()
		}(w)
	}

	// Broadcast to unblock all waiting goroutines
	g.Broadcast()

	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()
	for i := 0; i < count; i++ {
		select {
		case <-timer.C:
			t.Fatal("timeout: calls to wait did not unblock")
		case v := <-finished:
			if !v {
				t.Fatal("wait call returned false when broadcast was called")
			}
		}
	}
}

func TestWaiterGroup_Wait_Stop(t *testing.T) {
	t.Parallel()
	g := waiter.NewGroup()
	defer g.Stop()

	count := 10
	waiters := make([]waiter.Waiter, count)
	for i := range waiters {
		var err error
		waiters[i], err = g.NewWaiter()
		if err != nil {
			t.Fatal(err)
		}
	}

	finished := make(chan bool, count)
	for _, w := range waiters {
		go func(w waiter.Waiter) {
			finished <- w.Wait()
		}(w)
	}
	// Stop the waiter group
	g.Stop()

	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()
	for i := 0; i < count; i++ {
		select {
		case <-timer.C:
			t.Fatal("timeout: calls to wait did not unblock")
		case v := <-finished:
			if v {
				t.Fatal("wait call returned true when stop was called")
			}
		}
	}
}

func TestWaiterGroup_Wait_StopWaiter(t *testing.T) {
	t.Parallel()
	g := waiter.NewGroup()
	defer g.Stop()

	count := 10
	waiters := make([]waiter.Waiter, count)
	for i := range waiters {
		var err error
		waiters[i], err = g.NewWaiter()
		if err != nil {
			t.Fatal(err)
		}
	}

	finished := make(chan struct{}, count)
	for _, w := range waiters {
		go func(w waiter.Waiter) {
			for w.Wait() {
			}
			finished <- struct{}{}
		}(w)
	}

	// Stop the even waiters
	stopped := make(chan struct{})
	go func() {
		for i, w := range waiters {
			if i%2 == 0 {
				w.Stop()
			}
		}
		close(stopped)
	}()

	// Send many broadcasts
	for i := 0; i < 100; i++ {
		g.Broadcast()
	}

	<-stopped

	// Count the number of waiters that finished
	finishedCount := 0
COUNT:
	for i := 0; i < count; i++ {
		timer := time.NewTimer(10 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			break COUNT
		case <-finished:
			finishedCount++
		}
	}

	expectedCount := count / 2
	if finishedCount != expectedCount {
		t.Errorf("unexpected number of waiters finished: got %d exp %d", finishedCount, expectedCount)
	}
}
