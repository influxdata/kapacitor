package kapacitor

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

const (
	statCollected = "collected"
	statEmitted   = "emitted"

	defaultEdgeBufferSize = 1000
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

	logger     *log.Logger
	aborted    chan struct{}
	statsKey   string
	statMap    *expvar.Map
	groupMu    sync.RWMutex
	groupStats map[models.GroupID]*edgeStat
}

func newEdge(taskName, parentName, childName string, t pipeline.EdgeType, size int, logService LogService) *Edge {
	tags := map[string]string{
		"task":   taskName,
		"parent": parentName,
		"child":  childName,
		"type":   t.String(),
	}
	key, sm := NewStatistics("edges", tags)
	sm.Add(statCollected, 0)
	sm.Add(statEmitted, 0)
	e := &Edge{
		statsKey:   key,
		statMap:    sm,
		aborted:    make(chan struct{}),
		groupStats: make(map[models.GroupID]*edgeStat),
	}
	name := fmt.Sprintf("%s|%s->%s", taskName, parentName, childName)
	e.logger = logService.NewLogger(fmt.Sprintf("[edge:%s] ", name), log.LstdFlags)
	switch t {
	case pipeline.StreamEdge:
		e.stream = make(chan models.Point, size)
	case pipeline.BatchEdge:
		e.batch = make(chan models.Batch, size)
	case pipeline.ReduceEdge:
		e.reduce = make(chan *MapResult, size)
	}
	return e
}

func (e *Edge) emittedCount() int64 {
	c, err := strconv.ParseUint(e.statMap.Get(statEmitted).String(), 10, 64)
	if err != nil {
		panic("emitted count is not an int")
	}
	return int64(c)
}

func (e *Edge) collectedCount() int64 {
	c, err := strconv.ParseUint(e.statMap.Get(statCollected).String(), 10, 64)
	if err != nil {
		panic("collected count is not an int")
	}
	return int64(c)
}

// Stats for a given group for this edge
type edgeStat struct {
	collected int64
	emitted   int64
	tags      models.Tags
	dims      []string
}

// Get a snapshot of the current group statistics for this edge
func (e *Edge) readGroupStats(f func(group models.GroupID, collected, emitted int64, tags models.Tags, dims []string)) {
	e.groupMu.RLock()
	defer e.groupMu.RUnlock()
	for group, stats := range e.groupStats {
		f(
			group,
			stats.collected,
			stats.emitted,
			stats.tags,
			stats.dims,
		)
	}
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
	DeleteStatistics(e.statsKey)
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
			e.incEmitted(&p)
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
			e.incEmitted(&b)
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
	e.incCollected(&p)
	select {
	case <-e.aborted:
		return ErrAborted
	case e.stream <- p:
		return nil
	}
}

func (e *Edge) CollectBatch(b models.Batch) error {
	e.statMap.Add(statCollected, 1)
	e.incCollected(&b)
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

// Increment the emitted count of the group for this edge.
func (e *Edge) incEmitted(p models.PointInterface) {
	e.groupMu.Lock()
	defer e.groupMu.Unlock()
	if stats, ok := e.groupStats[p.PointGroup()]; ok {
		stats.emitted += 1
	} else {
		stats = &edgeStat{
			emitted: 1,
			tags:    p.PointTags(),
			dims:    p.PointDimensions(),
		}
		e.groupStats[p.PointGroup()] = stats
	}
}

// Increment the  ollected count of the group for this edge.
func (e *Edge) incCollected(p models.PointInterface) {
	e.groupMu.Lock()
	defer e.groupMu.Unlock()
	if stats, ok := e.groupStats[p.PointGroup()]; ok {
		stats.collected += 1
	} else {
		stats = &edgeStat{
			collected: 1,
			tags:      p.PointTags(),
			dims:      p.PointDimensions(),
		}
		e.groupStats[p.PointGroup()] = stats
	}
}
