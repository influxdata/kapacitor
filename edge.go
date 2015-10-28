package kapacitor

import (
	"errors"
	"expvar"
	"fmt"
	"log"
	"os"

	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/wlog"
)

const (
	statCollected = "collected"
	statEmitted   = "emitted"
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

	logger  *log.Logger
	closed  bool
	statMap *expvar.Map
}

func newEdge(name string, t pipeline.EdgeType) *Edge {
	sm := &expvar.Map{}
	sm.Init()
	sm.Add(statCollected, 0)
	sm.Add(statEmitted, 0)
	e := &Edge{statMap: sm}
	e.logger = wlog.New(os.Stderr, fmt.Sprintf("[edge:%s] ", name), log.LstdFlags)
	switch t {
	case pipeline.StreamEdge:
		e.stream = make(chan models.Point)
	case pipeline.BatchEdge:
		e.batch = make(chan models.Batch)
	case pipeline.ReduceEdge:
		e.reduce = make(chan *MapResult)
	}
	return e
}

func (e *Edge) Close() {
	if e.closed {
		return
	}
	e.closed = true
	e.logger.Printf(
		"I! closing c: %s e: %s\n",
		e.statMap.Get(statCollected),
		e.statMap.Get(statEmitted),
	)
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
	if wlog.LogLevel == wlog.DEBUG {
		// Explicitly check log level since this is expensive and frequent
		e.logger.Printf(
			"D! next point c: %s e: %s\n",
			e.statMap.Get(statCollected),
			e.statMap.Get(statEmitted),
		)
	}
	p, ok = <-e.stream
	if ok {
		e.statMap.Add(statEmitted, 1)
	}
	return
}

func (e *Edge) NextBatch() (b models.Batch, ok bool) {
	if wlog.LogLevel == wlog.DEBUG {
		// Explicitly check log level since this is expensive and frequent
		e.logger.Printf(
			"D! next batch c: %s e: %s\n",
			e.statMap.Get(statCollected),
			e.statMap.Get(statEmitted),
		)
	}
	b, ok = <-e.batch
	if ok {
		e.statMap.Add(statEmitted, 1)
	}
	return
}

func (e *Edge) NextMaps() (m *MapResult, ok bool) {
	if wlog.LogLevel == wlog.DEBUG {
		// Explicitly check log level since this is expensive and frequent
		e.logger.Printf(
			"D! next maps c: %s e: %s\n",
			e.statMap.Get(statCollected),
			e.statMap.Get(statEmitted),
		)
	}
	m, ok = <-e.reduce
	if ok {
		e.statMap.Add(statEmitted, 1)
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
	if wlog.LogLevel == wlog.DEBUG {
		// Explicitly check log level since this is expensive and frequent
		e.logger.Printf(
			"D! collect point c: %s e: %s\n",
			e.statMap.Get(statCollected),
			e.statMap.Get(statEmitted),
		)
	}
	e.statMap.Add(statCollected, 1)
	e.stream <- p
	return
}

func (e *Edge) CollectBatch(b models.Batch) (err error) {
	defer e.recover(&err)
	if wlog.LogLevel == wlog.DEBUG {
		// Explicitly check log level since this is expensive and frequent
		e.logger.Printf(
			"D! collect batch c: %s e: %s\n",
			e.statMap.Get(statCollected),
			e.statMap.Get(statEmitted),
		)
	}
	e.statMap.Add(statCollected, 1)
	e.batch <- b
	return
}

func (e *Edge) CollectMaps(m *MapResult) (err error) {
	defer e.recover(&err)
	if wlog.LogLevel == wlog.DEBUG {
		// Explicitly check log level since this is expensive and frequent
		e.logger.Printf(
			"D! collect maps c: %s e: %s\n",
			e.statMap.Get(statCollected),
			e.statMap.Get(statEmitted),
		)
	}
	e.statMap.Add(statCollected, 1)
	e.reduce <- m
	return
}
