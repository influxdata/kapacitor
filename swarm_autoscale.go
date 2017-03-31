package kapacitor

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/services/swarm/client"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
	"github.com/pkg/errors"
)

const (
	statsSwarmIncreaseEventsCount = "increase_events"
	statsSwarmDecreaseEventsCount = "decrease_events"
	statsSwarmCooldownDropsCount  = "cooldown_drops"
)

type SwarmAutoscaleNode struct {
	node
	k *pipeline.SwarmAutoscaleNode

	client client.Client

	replicasExprs     map[models.GroupID]stateful.Expression
	replicasScopePool stateful.ScopePool

	resourceStates map[string]swarmResourceState

	increaseCount      *expvar.Int
	decreaseCount      *expvar.Int
	cooldownDropsCount *expvar.Int

	replicasExprsMu sync.RWMutex

	min uint64
	max uint64
}

// Create a new SwarmAutoscaleNode which can trigger autoscale event for a Kubernetes cluster.
func newSwarmAutoscaleNode(et *ExecutingTask, n *pipeline.SwarmAutoscaleNode, l *log.Logger) (*SwarmAutoscaleNode, error) {
	client, err := et.tm.SwarmService.Client()
	if err != nil {
		return nil, fmt.Errorf("cannot use the SwarmAutoscale node, could not create Swarm client: %v", err)
	}
	kn := &SwarmAutoscaleNode{
		node:           node{Node: n, et: et, logger: l},
		k:              n,
		resourceStates: make(map[string]swarmResourceState),
		min:            uint64(n.Min),
		max:            uint64(n.Max),
		client:         client,
	}
	if kn.min < 1 {
		return nil, fmt.Errorf("minimum count must be >= 1, got %d", kn.min)
	}
	kn.node.runF = kn.runAutoscale
	// Initialize the replicas lambda expression scope pool
	if n.Replicas != nil {
		kn.replicasExprs = make(map[models.GroupID]stateful.Expression)
		kn.replicasScopePool = stateful.NewScopePool(ast.FindReferenceVariables(n.Replicas.Expression))
	}
	return kn, nil
}

func (k *SwarmAutoscaleNode) runAutoscale([]byte) error {
	valueF := func() int64 {
		k.replicasExprsMu.RLock()
		l := len(k.replicasExprs)
		k.replicasExprsMu.RUnlock()
		return int64(l)
	}
	k.statMap.Set(statCardinalityGauge, expvar.NewIntFuncGauge(valueF))

	k.increaseCount = &expvar.Int{}
	k.decreaseCount = &expvar.Int{}
	//errorsCount := &expvar.Int{}
	k.cooldownDropsCount = &expvar.Int{}

	k.statMap.Set(statsSwarmIncreaseEventsCount, k.increaseCount)
	k.statMap.Set(statsSwarmDecreaseEventsCount, k.decreaseCount)
	k.statMap.Set(statsSwarmCooldownDropsCount, k.cooldownDropsCount)

	switch k.Wants() {
	case pipeline.StreamEdge:
		for p, ok := k.ins[0].NextPoint(); ok; p, ok = k.ins[0].NextPoint() {
			k.timer.Start()
			if np, err := k.handlePoint(p.Name, p.Group, p.Dimensions, p.Time, p.Fields, p.Tags); err != nil {
				k.incrementErrorCount()
				k.logger.Println("E!", err)
			} else if np.Name != "" {
				k.timer.Pause()
				for _, child := range k.outs {
					err := child.CollectPoint(np)
					if err != nil {
						return err
					}
				}
				k.timer.Resume()
			}
			k.timer.Stop()
		}
	case pipeline.BatchEdge:
		for b, ok := k.ins[0].NextBatch(); ok; b, ok = k.ins[0].NextBatch() {
			k.timer.Start()
			for _, p := range b.Points {
				if np, err := k.handlePoint(b.Name, b.Group, b.PointDimensions(), p.Time, p.Fields, p.Tags); err != nil {
					k.incrementErrorCount()
					k.logger.Println("E!", err)
				} else if np.Name != "" {
					k.timer.Pause()
					for _, child := range k.outs {
						err := child.CollectPoint(np)
						if err != nil {
							return err
						}
					}
					k.timer.Resume()
				}
			}
			k.timer.Stop()
		}
	}
	return nil
}

type swarmResourceState struct {
	lastIncrease time.Time
	lastDecrease time.Time
	current      uint64
}

type swarmevent struct {
	Name string
	Old  uint64
	New  uint64
}

func (k *SwarmAutoscaleNode) handlePoint(streamName string, group models.GroupID, dims models.Dimensions, t time.Time, fields models.Fields, tags models.Tags) (models.Point, error) {
	name, err := k.getResourceFromPoint(tags)
	if err != nil {
		return models.Point{}, err
	}
	state, ok := k.resourceStates[name]
        if !ok {	
	        // If we haven't seen this resource before, get its state
		service, err := k.client.Get(name)
		if err != nil {
			return models.Point{}, errors.Wrapf(err, "could not determine initial scale for %s", name)
		}
		state.current = *service.Spec.Mode.Replicated.Replicas
		k.resourceStates[name] = state
	}

	// Eval the replicas expression
	k.replicasExprsMu.Lock()
	newReplicas, err := k.evalExpr(state.current, group, k.k.Replicas, k.replicasExprs, k.replicasScopePool, t, fields, tags)
	k.replicasExprsMu.Unlock()
	if err != nil {
		return models.Point{}, errors.Wrap(err, "failed to evaluate the replicas expression")
	}

	// Create the event
	e := swarmevent{
		Name: name,
		Old:  state.current,
		New:  newReplicas,
	}
	// Check bounds
	if k.max > 0 && e.New > k.max {
		e.New = k.max
	}
	if e.New < k.min {
		e.New = k.min
	}

	// Validate something changed
	if e.New == e.Old {
		// Nothing to do
		return models.Point{}, nil
	}

	// Update local copy of state
	change := e.New - e.Old
	state.current = e.New

	// Check last change cooldown times
	var counter *expvar.Int
	switch {
	case change > 0:
		if t.Before(state.lastIncrease.Add(k.k.IncreaseCooldown)) {
			// Still hot, nothing to do
			k.cooldownDropsCount.Add(1)
			return models.Point{}, nil
		}
		state.lastIncrease = t
		counter = k.increaseCount
	case change < 0:
		if t.Before(state.lastDecrease.Add(k.k.DecreaseCooldown)) {
			// Still hot, nothing to do
			k.cooldownDropsCount.Add(1)
			return models.Point{}, nil
		}
		state.lastDecrease = t
		counter = k.decreaseCount
	}

	// We have a valid event to apply
	if err := k.applyEvent(e); err != nil {
		return models.Point{}, errors.Wrap(err, "failed to apply scaling event")
	}

	// Only save the updated state if we were successful
	k.resourceStates[name] = state

	// Count event
	counter.Add(1)

	// Create new tags for the point.
	// Leave room for the namespace,kind, and resource tags.
	newTags := make(models.Tags, len(tags)+3)

	// Copy group by tags
	for _, d := range dims.TagNames {
		newTags[d] = tags[d]
	}

	// Create point representing the event
	p := models.Point{
		Name:       streamName,
		Time:       t,
		Group:      group,
		Dimensions: dims,
		Tags:       newTags,
		Fields: models.Fields{
			"old": int64(e.Old),
			"new": int64(e.New),
		},
	}
	return p, nil
}

func (k *SwarmAutoscaleNode) getResourceFromPoint(tags models.Tags) (name string, err error) {
	// Get the name of the resource
	switch {
        case k.k.ServiceName != "":
                t, ok := tags[k.k.ServiceName]
                if ok {
                        name = t
                }
	default:
		return "", errors.New("expected ServiceName to be set")
	}
	if name == "" {
		return "", errors.New("could not determine the name of the service")
	}
	return
}

func (k *SwarmAutoscaleNode) applyEvent(e swarmevent) error {
	k.logger.Printf("D! setting scale replicas to %d was %d for %s", e.New, e.Old, e.Name)
	service, err := k.client.Get(e.Name)
	if err != nil {
		return err
	}
	if *service.Spec.Mode.Replicated.Replicas != e.Old {
		k.logger.Printf("W! the Swarm scale spec and Kapacitor's spec do not match for resource %s, did it change externally?", e.Name)
	}
	var replicaCount uint64 = e.New
	service.Spec.Mode.Replicated.Replicas = &replicaCount
	if err := k.client.Update(service); err != nil {
		return errors.Wrapf(err, "failed to update the scale for resource %s", e.Name)
	}
	return nil
}

func (k *SwarmAutoscaleNode) evalExpr(
	current uint64,
	group models.GroupID,
	lambda *ast.LambdaNode,
	expressionsMap map[models.GroupID]stateful.Expression,
	pool stateful.ScopePool,
	t time.Time,
	fields models.Fields,
	tags models.Tags,
) (uint64, error) {
	expr, ok := expressionsMap[group]
	if !ok {
		var err error
		expr, err = stateful.NewExpression(lambda.Expression)
		if err != nil {
			return 0, err
		}
		expressionsMap[group] = expr
	}
	i, err := k.evalInt(current, expr, pool, t, fields, tags)
	return i, err
}

// evalInt - Evaluate a given expression as an int64 against a set of fields and tags.
// The CurrentField is also set on the scope if not empty.
func (k *SwarmAutoscaleNode) evalInt(current uint64, se stateful.Expression, scopePool stateful.ScopePool, now time.Time, fields models.Fields, tags models.Tags) (uint64, error) {
	vars := scopePool.Get()
	defer scopePool.Put(vars)

	// Set the current replicas value on the scope if requested.
	if k.k.CurrentField != "" {
		vars.Set(k.k.CurrentField, current)
	}

	// Fill the scope with the rest of the values
	err := fillScope(vars, scopePool.ReferenceVariables(), now, fields, tags)
	if err != nil {
		return 0, err
	}

	i, err := se.EvalInt(vars)
	if err != nil {
		return 0, err
	}
	return uint64(i), nil
}
