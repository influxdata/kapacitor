package kapacitor

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/wlog"
)

type StreamCollector interface {
	CollectPoint(*models.Point) error
	Close()
}

type BatchCollector interface {
	CollectBatch([]*models.Point) error
	Close()
}

type Edge struct {
	stream chan *models.Point
	batch  chan []*models.Point
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
		return &Edge{stream: make(chan *models.Point), logger: l}
	case pipeline.BatchEdge:
		return &Edge{batch: make(chan []*models.Point), logger: l}
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

func (e *Edge) NextPoint() *models.Point {
	e.logger.Printf("D! next point c: %d e: %d\n", e.collected, e.emitted)
	p := <-e.stream
	if p != nil {
		e.emitted++
	}
	return p
}

func (e *Edge) NextBatch() []*models.Point {
	e.logger.Printf("D! next batch c: %d e: %d\n", e.collected, e.emitted)
	b := <-e.batch
	if b != nil {
		e.emitted++
	}
	return b
}

func (e *Edge) NextMaps() *MapResult {
	e.logger.Printf("D! next maps c: %d e: %d\n", e.collected, e.emitted)
	m := <-e.reduce
	if m != nil {
		e.emitted++
	}
	return m
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

func (e *Edge) CollectPoint(p *models.Point) (err error) {
	defer e.recover(&err)
	e.logger.Printf("D! collect point c: %d e: %d\n", e.collected, e.emitted)
	e.collected++
	e.stream <- p
	return
}

func (e *Edge) CollectBatch(b []*models.Point) (err error) {
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

type streamItr struct {
	sync.Mutex
	field string
	tags  map[string]string
	batch []*models.Point
	i     int
}

func newItr(batch []*models.Point, field string) *streamItr {
	return &streamItr{
		batch: batch,
		field: field,
	}
}

func (s *streamItr) Next() (time int64, value interface{}) {
	s.Lock()
	i := s.i
	s.i++
	s.Unlock()
	if i >= len(s.batch) {
		return -1, nil
	}
	p := s.batch[i]
	if p == nil {
		return -1, nil
	}
	time = p.Time.Unix()
	value = p.Fields[s.field]
	return
}

func (s *streamItr) Tags() map[string]string {
	return s.tags
}

func (s *streamItr) TMin() int64 {
	return 0
}
