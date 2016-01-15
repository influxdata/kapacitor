package kapacitor

import (
	"errors"
	"expvar"
	"fmt"
	"log"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

const (
	statCollected = "collected"
	statEmitted   = "emitted"
)

var ErrAborted = errors.New("edged aborted")

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
	aborted chan struct{}
	statMap *expvar.Map
}

func newEdge(taskName, parentName, childName string, t pipeline.EdgeType, logService LogService) *Edge {
	tags := map[string]string{
		"task":   taskName,
		"parent": parentName,
		"child":  childName,
		"type":   t.String(),
	}
	sm := NewStatistics("edges", tags)
	sm.Add(statCollected, 0)
	sm.Add(statEmitted, 0)
	e := &Edge{statMap: sm, aborted: make(chan struct{})}
	name := fmt.Sprintf("%s|%s->%s", taskName, parentName, childName)
	e.logger = logService.NewLogger(fmt.Sprintf("[edge:%s] ", name), log.LstdFlags)
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

func (e *Edge) collectedCount() string {
	return e.statMap.Get(statCollected).String()
}

// Close the edge, this can only be called after all
// collect calls to the edge have finished.
func (e *Edge) Close() {
	e.logger.Printf(
		"D! closing c: %s e: %s\n",
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

// Abort all next and collect calls.
// Items in flight may or may not be processed.
func (e *Edge) Abort() {
	close(e.aborted)
	e.logger.Printf(
		"I! aborting c: %s e: %s\n",
		e.statMap.Get(statCollected),
		e.statMap.Get(statEmitted),
	)
}

func (e *Edge) Next() (p models.PointInterface, ok bool) {
	if e.stream != nil {
		return e.NextPoint()
	}
	return e.NextBatch()
}

func (e *Edge) NextPoint() (p models.Point, ok bool) {
	select {
	case <-e.aborted:
	case p, ok = <-e.stream:
		if ok {
			e.statMap.Add(statEmitted, 1)
		}
	}
	return
}

func (e *Edge) NextBatch() (b models.Batch, ok bool) {
	select {
	case <-e.aborted:
	case b, ok = <-e.batch:
		if ok {
			e.statMap.Add(statEmitted, 1)
		}
	}
	return
}

func (e *Edge) NextMaps() (m *MapResult, ok bool) {
	select {
	case <-e.aborted:
	case m, ok = <-e.reduce:
		if ok {
			e.statMap.Add(statEmitted, 1)
		}
	}
	return
}

func (e *Edge) CollectPoint(p models.Point) error {
	e.statMap.Add(statCollected, 1)
	select {
	case <-e.aborted:
		return ErrAborted
	case e.stream <- p:
		return nil
	}
}

func (e *Edge) CollectBatch(b models.Batch) error {
	e.statMap.Add(statCollected, 1)
	select {
	case <-e.aborted:
		return ErrAborted
	case e.batch <- b:
		return nil
	}
}

func (e *Edge) CollectMaps(m *MapResult) error {
	e.statMap.Add(statCollected, 1)
	select {
	case <-e.aborted:
		return ErrAborted
	case e.reduce <- m:
		return nil
	}
}
