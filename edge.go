package kapacitor

import (
	"errors"
	"fmt"
	"log"
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

type Edge struct {
	edge.StatsEdge

	mu     sync.Mutex
	closed bool

	statsKey string
	statMap  *expvar.Map
	logger   *log.Logger
}

func newEdge(taskName, parentName, childName string, t pipeline.EdgeType, size int, logService LogService) edge.StatsEdge {
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
	name := fmt.Sprintf("%s|%s->%s", taskName, parentName, childName)
	return &Edge{
		StatsEdge: e,
		statsKey:  key,
		statMap:   sm,
		logger:    logService.NewLogger(fmt.Sprintf("[edge:%s] ", name), log.LstdFlags),
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
	e.logger.Printf("D! closing c: %d e: %d",
		e.Collected(),
		e.Emitted(),
	)
	return e.StatsEdge.Close()
}
