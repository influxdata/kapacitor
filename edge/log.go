// +build debug

package edge

import (
	"log"

	"github.com/influxdata/kapacitor/pipeline"
)

type logEdge struct {
	e      Edge
	logger *log.Logger
}

// NewLogEdge creates an edge that logs the type of all collected and emitted messages.
//
// This edge should only be used during debug sessions and not in production code.
// As such by default build tags exclude this file from being compiled.
// Add the `-tags debug` arguments to build or test commands in order to include this file for compilation.
func NewLogEdge(l *log.Logger, e Edge) Edge {
	return &logEdge{
		e:      e,
		logger: l,
	}
}

func (e *logEdge) Collect(m Message) error {
	e.logger.Println("D! collect:", m.Type())
	return e.e.Collect(m)
}

func (e *logEdge) Emit() (m Message, ok bool) {
	m, ok = e.e.Emit()
	if ok {
		e.logger.Println("D! emit:", m.Type())
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
