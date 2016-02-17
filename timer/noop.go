package timer

import "time"

type noop struct {
}

func (noop) Start()                            {}
func (noop) Pause()                            {}
func (noop) Resume()                           {}
func (noop) Stop()                             {}
func (noop) AverageTime() (time.Duration, int) { return 0, 1 }

func NewNoOp() Timer {
	return noop{}
}
