// +build debug

package edge

import (
	"github.com/influxdata/kapacitor/pipeline"
)

type Diagnostic interface {
	Collect(mtype MessageType)
	Emit(mtype MessageType)
}

type logEdge struct {
	e    Edge
	diag Diagnostic
}

// NewLogEdge creates an edge that logs the type of all collected and emitted messages.
//
// This edge should only be used during debug sessions and not in production code.
// As such by default build tags exclude this file from being compiled.
// Add the `-tags debug` arguments to build or test commands in order to include this file for compilation.
func NewLogEdge(d Diagnostic, e Edge) Edge {
	return &logEdge{
		e:    e,
		diag: d,
	}
}

func (e *logEdge) Collect(m Message) error {
	e.diag.Collect(m.Type())
	return e.e.Collect(m)
}

func (e *logEdge) Emit() (m Message, ok bool) {
	m, ok = e.e.Emit()
	if ok {
		e.diag.Emit(m.Type())
	}
	return
}

func (e *logEdge) Close() error {
	return e.e.Close()
}

func (e *logEdge) Abort() {
	e.e.Abort()
}

func (e *logEdge) Type() pipeline.EdgeType {
	return e.e.Type()
}
