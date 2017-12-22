package kapacitor

import (
	"errors"
	"sync"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/server/vars"
)

const (
	statCollected = "collected"
	statEmitted   = "emitted"

	defaultEdgeBufferSize = 1000
)

var ErrAborted = errors.New("edged aborted")

type EdgeDiagnostic interface {
	ClosingEdge(collected, emitted int64)
}

type Edge struct {
	edge.StatsEdge

	mu     sync.Mutex
	closed bool

	statsKey string
	statMap  *expvar.Map
	diag     EdgeDiagnostic
}

func newEdge(taskName, parentName, childName string, t pipeline.EdgeType, size int, d EdgeDiagnostic) edge.StatsEdge {
	e := edge.NewStatsEdge(edge.NewChannelEdge(t, defaultEdgeBufferSize))
	tags := map[string]string{
		"task":   taskName,
		"parent": parentName,
		"child":  childName,
		"type":   t.String(),
	}
	key, sm := vars.NewStatistic("edges", tags)
	sm.Set(statCollected, e.CollectedVar())
	sm.Set(statEmitted, e.EmittedVar())
	return &Edge{
		StatsEdge: e,
		statsKey:  key,
		statMap:   sm,
		diag:      d,
	}
}

func (e *Edge) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	vars.DeleteStatistic(e.statsKey)
	e.diag.ClosingEdge(e.Collected(), e.Emitted())
	return e.StatsEdge.Close()
}
