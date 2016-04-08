package udf

import "github.com/influxdata/kapacitor/models"

// Interface for communicating with a UDF
type Interface interface {
	Open() error
	Info() (Info, error)
	Init(options []*Option) error
	Abort(err error)
	Close() error

	Snapshot() ([]byte, error)
	Restore(snapshot []byte) error

	PointIn() chan<- models.Point
	BatchIn() chan<- models.Batch
	PointOut() <-chan models.Point
	BatchOut() <-chan models.Batch
}
