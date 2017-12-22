package clock_test

import (
	"testing"
	"time"

	"github.com/influxdata/kapacitor/clock"
)

func TestClockUntilSleepFirst(t *testing.T) {

	c := clock.New(time.Time{})
	zero := c.Zero()

	done := make(chan bool)
	go func() {
		zero := c.Zero()

		til := zero.Add(10 * time.Microsecond)
		c.Until(til)
		done <- true

	}()

	select {
	case <-done:
		t.Fatal("unexpected return from c.Until")
	case <-time.After(10 * time.Millisecond):
	}

	c.Set(zero.Add(9 * time.Microsecond))
	select {
	case <-done:
		t.Fatal("unexpected return from c.Until")
	case <-time.After(10 * time.Millisecond):
	}

	c.Set(zero.Add(10 * time.Microsecond))
	select {
	case <-done:
	case <-time.After(20 * time.Millisecond):
		t.Fatal("expected return from c.Until")
	}
}

func TestClockUntilNoSleep(t *testing.T) {

	c := clock.New(time.Time{})
	zero := c.Zero()

	done := make(chan bool)
	go func() {
		zero := c.Zero()

		til := zero.Add(10 * time.Microsecond)
		c.Until(til)
		done <- true
	}()

	c.Set(zero.Add(10 * time.Microsecond))
	select {
	case <-done:
	case <-time.After(10 * time.Millisecond):
		t.Fatal("expected return from c.Until")
	}
}

func TestClockUntilStart(t *testing.T) {

	c := clock.New(time.Unix(0, 0))
	zero := c.Zero()

	done := make(chan bool)
	go func() {
		zero := c.Zero()

		til := zero.Add(10 * time.Microsecond)
		c.Until(til)
		done <- true
	}()

	c.Set(zero.Add(10 * time.Microsecond))
	select {
	case <-done:
	case <-time.After(10 * time.Millisecond):
		t.Fatal("expected return from c.Until")
	}
}

func TestClockUntilMultiple(t *testing.T) {

	c := clock.New(time.Time{})

	done := make(chan bool, 10)
	go func() {
		til := c.Zero()

		for i := 0; i < 10; i++ {
			til = til.Add(10 * time.Microsecond)
			c.Until(til)
			done <- true
		}
	}()

	now := c.Zero()
	for i := 0; i < 5; i++ {
		now = now.Add(20 * time.Microsecond)
		c.Set(now)
		select {
		case <-done:
		case <-time.After(10 * time.Millisecond):
			t.Fatal("expected return from c.Until")
		}
		select {
		case <-done:
		case <-time.After(10 * time.Millisecond):
			t.Fatal("expected return from c.Until")
		}
	}
}

func TestClockUntilMultipleGos(t *testing.T) {

	c := clock.New(time.Time{})

	count := 10

	done := make(chan bool, count)
	for i := 0; i < count; i++ {
		go func(i int) {
			c.Until(c.Zero().Add(time.Duration(i) * time.Second))
			done <- true
		}(i)
	}

	now := c.Zero()
	for i := 0; i < count; i++ {
		now = now.Add(time.Duration(i) * time.Second)
		c.Set(now)
		select {
		case <-done:
		case <-time.After(20 * time.Millisecond):
			t.Fatalf("expected return from c.Until i: %d", i)
		}
	}
}

func TestWallClock(t *testing.T) {

	c := clock.Wall()

	done := make(chan bool, 1)
	go func() {
		zero := time.Now()
		til := zero.Add(10 * time.Millisecond)
		c.Until(til)
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("expected return from c.Until")
	}
}
