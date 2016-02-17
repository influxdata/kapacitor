package timer

import (
	"math/rand"
	"sync"
	"time"
)

type Timer interface {
	// Start the timer
	// Timer must be stopped, which is the state of a new timer.
	Start()
	// Pause the timer.
	// Timer must be started.
	Pause()
	// Resumed the timer.
	// Timer must be paused.
	Resume()
	// Stop the timer.
	// Timer must be started.
	Stop()
	// Return average of timings and number of
	// timings used to compute the average.
	AverageTime() (time.Duration, int)
}

type timerState int

const (
	Stopped timerState = iota
	Started
	Paused
)

// Perform basic timings of sections of code.
// Keeps a running average of timing values.
type timer struct {
	sampleRate float64
	start      time.Time
	current    time.Duration
	avg        *movavg
	state      timerState
}

func New(sampleRate float64, movingAverageSize int) Timer {
	return &timer{
		sampleRate: sampleRate,
		avg:        newMovAvg(movingAverageSize),
	}
}

// Start timer.
func (t *timer) Start() {
	if t.state != Stopped {
		panic("invalid timer state")
	}
	if rand.Float64() < t.sampleRate {
		t.state = Started
		t.start = time.Now()
	}
}

// Pause current timing event.
func (t *timer) Pause() {
	if t.state != Started {
		return
	}
	t.current += time.Now().Sub(t.start)
	t.state = Paused
}

// Resumed paused timer.
func (t *timer) Resume() {
	if t.state != Paused {
		return
	}
	t.start = time.Now()
	t.state = Started
}

// Stop and record time of event.
// The moving average is updated at this point.
func (t *timer) Stop() {
	if t.state != Started {
		return
	}
	t.current += time.Now().Sub(t.start)
	t.avg.update(float64(t.current))
	t.current = 0
	t.state = Stopped
}

// Return the average time in nanoseconds and
// the number of timings used to compute the average.
func (t *timer) AverageTime() (time.Duration, int) {
	avg, count := t.avg.average()
	return time.Duration(avg), count
}

// Maintains a moving average of values
type movavg struct {
	size    int
	history []float64
	idx     int
	count   int
	avg     float64
	mu      sync.RWMutex
}

func newMovAvg(size int) *movavg {
	return &movavg{
		size:    size,
		history: make([]float64, size),
		idx:     -1,
	}
}

func (m *movavg) update(value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := float64(m.count)
	if n == 0 {
		m.count = 1
		m.avg = value
		return
	}

	m.avg += (value - m.avg) / n
	m.idx = (m.idx + 1) % m.size

	if m.count == m.size {
		old := m.history[m.idx]
		m.avg = (n*m.avg - old) / (n - 1)
	} else {
		m.count++
	}
	m.history[m.idx] = value
}

func (m *movavg) average() (float64, int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.avg, m.count
}
