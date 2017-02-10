package kapacitor

import (
	"errors"
	"fmt"
	"sync"

	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/vars"
	"github.com/uber-go/zap"
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
	mu     sync.Mutex
	closed bool

	stream chan models.Point
	batch  chan models.Batch

	logger     zap.Logger
	aborted    chan struct{}
	statsKey   string
	collected  *expvar.Int
	emitted    *expvar.Int
	statMap    *expvar.Map
	groupMu    sync.RWMutex
	groupStats map[models.GroupID]*edgeStat
}

func newEdge(taskName, parentName, childName string, t pipeline.EdgeType, size int, parentLogger zap.Logger) *Edge {
	tags := map[string]string{
		"task":   taskName,
		"parent": parentName,
		"child":  childName,
		"type":   t.String(),
	}
	key, sm := vars.NewStatistic("edges", tags)
	collected := &expvar.Int{}
	emitted := &expvar.Int{}
	sm.Set(statCollected, collected)
	sm.Set(statEmitted, emitted)
	e := &Edge{
		statsKey:   key,
		statMap:    sm,
		collected:  collected,
		emitted:    emitted,
		aborted:    make(chan struct{}),
		groupStats: make(map[models.GroupID]*edgeStat),
	}
	name := fmt.Sprintf("%s->%s", parentName, childName)
	e.logger = parentLogger.With(zap.String("edge", name), zap.String("type", t.String()))
	switch t {
	case pipeline.StreamEdge:
		e.stream = make(chan models.Point, size)
	case pipeline.BatchEdge:
		e.batch = make(chan models.Batch, size)
	}
	return e
}

func (e *Edge) emittedCount() int64 {
	return e.emitted.IntValue()
}

func (e *Edge) collectedCount() int64 {
	return e.collected.IntValue()
}

// Stats for a given group for this edge
type edgeStat struct {
	collected int64
	emitted   int64
	tags      models.Tags
	dims      models.Dimensions
}

// Get a snapshot of the current group statistics for this edge
func (e *Edge) readGroupStats(f func(group models.GroupID, collected, emitted int64, tags models.Tags, dims models.Dimensions)) {
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
// Can be called multiple times.
func (e *Edge) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	e.closed = true
	e.logger.Debug(
		"closing edge",
		zap.Int64("collected", e.collected.IntValue()),
		zap.Int64("emitted", e.emitted.IntValue()),
	)
	if e.stream != nil {
		close(e.stream)
	}
	if e.batch != nil {
		close(e.batch)
	}
	vars.DeleteStatistic(e.statsKey)
}

// Abort all next and collect calls.
// Items in flight may or may not be processed.
func (e *Edge) Abort() {
	close(e.aborted)
	e.logger.Error(
		"aborting edge",
		zap.Int64("collected", e.collected.IntValue()),
		zap.Int64("emitted", e.emitted.IntValue()),
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
			e.emitted.Add(1)
			e.incEmitted(p.Group, p.Tags, p.Dimensions, 1)
		}
	}
	return
}

func (e *Edge) NextBatch() (b models.Batch, ok bool) {
	select {
	case <-e.aborted:
	case b, ok = <-e.batch:
		if ok {
			e.emitted.Add(1)
			e.incEmitted(b.Group, b.Tags, b.PointDimensions(), int64(len(b.Points)))
		}
	}
	return
}

func (e *Edge) CollectPoint(p models.Point) error {
	e.collected.Add(1)
	e.incCollected(p.Group, p.Tags, p.Dimensions, 1)
	select {
	case <-e.aborted:
		return ErrAborted
	case e.stream <- p:
		return nil
	}
}

func (e *Edge) CollectBatch(b models.Batch) error {
	e.collected.Add(1)
	e.incCollected(b.Group, b.Tags, b.PointDimensions(), int64(len(b.Points)))
	select {
	case <-e.aborted:
		return ErrAborted
	case e.batch <- b:
		return nil
	}
}

// Increment the emitted count of the group for this edge.
func (e *Edge) incEmitted(group models.GroupID, tags models.Tags, dims models.Dimensions, count int64) {
	// we are "manually" calling Unlock() and not using defer, because this method is called
	// in hot locations (NextPoint/CollectPoint) and defer have some performance penalty
	e.groupMu.Lock()

	if stats, ok := e.groupStats[group]; ok {
		stats.emitted += count
		e.groupMu.Unlock()
	} else {
		stats = &edgeStat{
			emitted: count,
			tags:    tags,
			dims:    dims,
		}
		e.groupStats[group] = stats
		e.groupMu.Unlock()
	}
}

// Increment the collected count of the group for this edge.
func (e *Edge) incCollected(group models.GroupID, tags models.Tags, dims models.Dimensions, count int64) {
	// we are "manually" calling Unlock() and not using defer, because this method is called
	// in hot locations (NextPoint/CollectPoint) and defer have some performance penalty
	e.groupMu.Lock()

	if stats, ok := e.groupStats[group]; ok {
		stats.collected += count
		e.groupMu.Unlock()
	} else {
		stats = &edgeStat{
			collected: count,
			tags:      tags,
			dims:      dims,
		}
		e.groupStats[group] = stats
		e.groupMu.Unlock()
	}
}
