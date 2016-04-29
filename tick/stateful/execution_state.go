package stateful

import "github.com/influxdata/kapacitor/tick"

// ExecutionState is auxiliary struct for data/context that needs to be passed
// to evaluation functions
type ExecutionState struct {
	Funcs tick.Funcs
}

func CreateExecutionState() ExecutionState {
	return ExecutionState{
		Funcs: tick.NewFunctions(),
	}
}

func (ea ExecutionState) ResetAll() {
	// Reset the functions
	for _, f := range ea.Funcs {
		f.Reset()
	}
}
