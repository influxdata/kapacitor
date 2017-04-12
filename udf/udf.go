package udf

import (
	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/udf/agent"
)

// Interface for communicating with a UDF
type Interface interface {
	Open() error
	Info() (Info, error)
	Init(options []*agent.Option) error
	Abort(err error)
	Close() error

	Snapshot() ([]byte, error)
	Restore(snapshot []byte) error

	In() chan<- edge.Message
	Out() <-chan edge.Message
}
