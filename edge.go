package kapacitor

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/wlog"
)

type StreamCollector interface {
	CollectPoint(models.Point) error
	Close()
}

type BatchCollector interface {
	CollectBatch(models.Batch) error
	Close()
}

type Edge struct {
	stream chan models.Point
	batch  chan models.Batch
	reduce chan *MapResult

	collected int64
	emitted   int64
	logger    *log.Logger
	closed    bool
}

func newEdge(name string, t pipeline.EdgeType) *Edge {
	l := wlog.New(os.Stderr, fmt.Sprintf("[edge:%s] ", name), log.LstdFlags)
	switch t {
	case pipeline.StreamEdge:
		return &Edge{stream: make(chan models.Point), logger: l}
	case pipeline.BatchEdge:
		return &Edge{batch: make(chan models.Batch), logger: l}
	case pipeline.ReduceEdge:
		return &Edge{reduce: make(chan *MapResult), logger: l}
	}
	return nil

}

func (e *Edge) Close() {
	if e.closed {
		return
	}
	e.closed = true
	e.logger.Printf("I! closing c: %d e: %d\n", e.collected, e.emitted)
	if e.stream != nil {
		close(e.stream)
	}
	if e.batch != nil {
		close(e.batch)
	}
	if e.reduce != nil {
		close(e.reduce)
	}
}

func (e *Edge) NextPoint() (p models.Point, ok bool) {
	e.logger.Printf("D! next point c: %d e: %d\n", e.collected, e.emitted)
	p, ok = <-e.stream
	if ok {
		e.emitted++
	}
	return
}

func (e *Edge) NextBatch() (b models.Batch, ok bool) {
	e.logger.Printf("D! next batch c: %d e: %d\n", e.collected, e.emitted)
	b, ok = <-e.batch
	if ok {
		e.emitted++
	}
	return
}

func (e *Edge) NextMaps() (m *MapResult, ok bool) {
	e.logger.Printf("D! next maps c: %d e: %d\n", e.collected, e.emitted)
	m, ok = <-e.reduce
	if m != nil {
		e.emitted++
	}
	return
}

func (e *Edge) recover(errp *error) {
	if r := recover(); r != nil {
		msg := fmt.Sprintf("%s", r)
		if msg == "send on closed channel" {
			*errp = errors.New(msg)
		} else {
			panic(r)
		}
	}
}

func (e *Edge) CollectPoint(p models.Point) (err error) {
	defer e.recover(&err)
	e.logger.Printf("D! collect point c: %d e: %d\n", e.collected, e.emitted)
	e.collected++
	e.stream <- p
	return
}

func (e *Edge) CollectBatch(b models.Batch) (err error) {
	defer e.recover(&err)
	e.logger.Printf("D! collect batch c: %d e: %d\n", e.collected, e.emitted)
	e.collected++
	e.batch <- b
	return
}

func (e *Edge) CollectMaps(m *MapResult) (err error) {
	defer e.recover(&err)
	e.logger.Printf("D! collect maps c: %d e: %d\n", e.collected, e.emitted)
	e.collected++
	e.reduce <- m
	return
}
