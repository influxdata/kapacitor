package kapacitor

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

type stateTracker interface {
	track(p models.BatchPoint, inState bool) interface{}
	reset()
}

type stateTrackingGroup struct {
	stateful.Expression
	stateful.ScopePool
	tracker stateTracker
}

type StateTrackingNode struct {
	node
	lambda *ast.LambdaNode
	as     string

	newTracker func() stateTracker

	groupsMu sync.RWMutex
	groups   map[models.GroupID]*stateTrackingGroup
}

func (stn *StateTrackingNode) group(g models.GroupID) (*stateTrackingGroup, error) {
	stn.groupsMu.RLock()
	stg := stn.groups[g]
	stn.groupsMu.RUnlock()

	if stg == nil {
		// Grab the write lock
		stn.groupsMu.Lock()
		defer stn.groupsMu.Unlock()

		// Check again now that we have the write lock
		stg = stn.groups[g]
		if stg == nil {
			// Create a new tracking group
			stg = &stateTrackingGroup{}

			var err error
			stg.Expression, err = stateful.NewExpression(stn.lambda.Expression)
			if err != nil {
				return nil, fmt.Errorf("Failed to compile expression: %v", err)
			}

			stg.ScopePool = stateful.NewScopePool(ast.FindReferenceVariables(stn.lambda.Expression))

			stg.tracker = stn.newTracker()

			stn.groups[g] = stg
		}
	}
	return stg, nil
}

func (stn *StateTrackingNode) runStateTracking(_ []byte) error {
	// Setup working_cardinality gauage.
	valueF := func() int64 {
		stn.groupsMu.RLock()
		l := len(stn.groups)
		stn.groupsMu.RUnlock()
		return int64(l)
	}
	stn.statMap.Set(statCardinalityGauge, expvar.NewIntFuncGauge(valueF))

	switch stn.Provides() {
	case pipeline.StreamEdge:
		for p, ok := stn.ins[0].NextPoint(); ok; p, ok = stn.ins[0].NextPoint() {
			stn.timer.Start()
			stg, err := stn.group(p.Group)
			if err != nil {
				return err
			}

			pass, err := EvalPredicate(stg.Expression, stg.ScopePool, p.Time, p.Fields, p.Tags)
			if err != nil {
				stn.incrementErrorCount()
				stn.logger.Println("E! error while evaluating expression:", err)
				stn.timer.Stop()
				continue
			}

			p.Fields = p.Fields.Copy()
			p.Fields[stn.as] = stg.tracker.track(models.BatchPointFromPoint(p), pass)

			stn.timer.Stop()
			for _, child := range stn.outs {
				err := child.CollectPoint(p)
				if err != nil {
					return err
				}
			}
		}
	case pipeline.BatchEdge:
		for b, ok := stn.ins[0].NextBatch(); ok; b, ok = stn.ins[0].NextBatch() {
			stn.timer.Start()

			stg, err := stn.group(b.Group)
			if err != nil {
				return err
			}
			stg.tracker.reset()

			b.Points = b.ShallowCopyPoints()
			for i := 0; i < len(b.Points); {
				p := &b.Points[i]
				pass, err := EvalPredicate(stg.Expression, stg.ScopePool, p.Time, p.Fields, p.Tags)
				if err != nil {
					stn.incrementErrorCount()
					stn.logger.Println("E! error while evaluating epression:", err)
					b.Points = append(b.Points[:i], b.Points[i+1:]...)
					continue
				}
				i++

				p.Fields = p.Fields.Copy()
				p.Fields[stn.as] = stg.tracker.track(*p, pass)
			}

			stn.timer.Stop()
			for _, child := range stn.outs {
				err := child.CollectBatch(b)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

type stateDurationTracker struct {
	sd *pipeline.StateDurationNode

	startTime time.Time
}

func (sdt *stateDurationTracker) reset() {
	sdt.startTime = time.Time{}
}

func (sdt *stateDurationTracker) track(p models.BatchPoint, inState bool) interface{} {
	if !inState {
		sdt.startTime = time.Time{}
		return float64(-1)
	}

	if sdt.startTime.IsZero() {
		sdt.startTime = p.Time
	}
	return float64(p.Time.Sub(sdt.startTime)) / float64(sdt.sd.Unit)
}

func newStateDurationNode(et *ExecutingTask, sd *pipeline.StateDurationNode, l *log.Logger) (*StateTrackingNode, error) {
	if sd.Lambda == nil {
		return nil, fmt.Errorf("nil expression passed to StateDurationNode")
	}
	stn := &StateTrackingNode{
		node:   node{Node: sd, et: et, logger: l},
		lambda: sd.Lambda,
		as:     sd.As,

		groups:     make(map[models.GroupID]*stateTrackingGroup),
		newTracker: func() stateTracker { return &stateDurationTracker{sd: sd} },
	}
	stn.node.runF = stn.runStateTracking
	return stn, nil
}

type stateCountTracker struct {
	count int64
}

func (sct *stateCountTracker) reset() {
	sct.count = 0
}

func (sct *stateCountTracker) track(p models.BatchPoint, inState bool) interface{} {
	if !inState {
		sct.count = 0
		return int64(-1)
	}

	sct.count++
	return sct.count
}

func newStateCountNode(et *ExecutingTask, sc *pipeline.StateCountNode, l *log.Logger) (*StateTrackingNode, error) {
	if sc.Lambda == nil {
		return nil, fmt.Errorf("nil expression passed to StateCountNode")
	}
	stn := &StateTrackingNode{
		node:   node{Node: sc, et: et, logger: l},
		lambda: sc.Lambda,
		as:     sc.As,

		groups:     make(map[models.GroupID]*stateTrackingGroup),
		newTracker: func() stateTracker { return &stateCountTracker{} },
	}
	stn.node.runF = stn.runStateTracking
	return stn, nil
}
