package timer

type noop struct {
}

func (noop) Start()  {}
func (noop) Pause()  {}
func (noop) Resume() {}
func (noop) Stop()   {}

func NewNoOp() Timer {
	return noop{}
}
