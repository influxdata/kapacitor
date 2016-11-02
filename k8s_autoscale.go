package kapacitor

import (
	"fmt"
	"log"
	"time"

	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/services/k8s/client"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
	"github.com/pkg/errors"
)

const (
	statsK8sIncreaseEventsCount = "increase_events"
	statsK8sDecreaseEventsCount = "decrease_events"
	statsK8sErrorsCount         = "errors"
	statsK8sCooldownDropsCount  = "cooldown_drops"
)

type K8sAutoscaleNode struct {
	node
	k *pipeline.K8sAutoscaleNode

	client client.Client

	replicasExprs     map[models.GroupID]stateful.Expression
	replicasScopePool stateful.ScopePool

	resourceStates map[string]resourceState

	increaseCount      *expvar.Int
	decreaseCount      *expvar.Int
	cooldownDropsCount *expvar.Int

	min int
	max int
}

// Create a new K8sAutoscaleNode which can trigger autoscale event for a Kubernetes cluster.
func newK8sAutoscaleNode(et *ExecutingTask, n *pipeline.K8sAutoscaleNode, l *log.Logger) (*K8sAutoscaleNode, error) {
	client, err := et.tm.K8sService.Client()
	if err != nil {
		return nil, fmt.Errorf("cannot use the k8sAutoscale node, could not create kubernetes client: %v", err)
	}
	kn := &K8sAutoscaleNode{
		node:           node{Node: n, et: et, logger: l},
		k:              n,
		resourceStates: make(map[string]resourceState),
		min:            int(n.Min),
		max:            int(n.Max),
		client:         client,
	}
	if kn.min < 1 {
		return nil, fmt.Errorf("minimum count must be >= 1, got %d", kn.min)
	}
	kn.node.runF = kn.runAutoscale
	// Initialize the replicas lambda expression scope pool
	if n.Replicas != nil {
		kn.replicasExprs = make(map[models.GroupID]stateful.Expression)
		kn.replicasScopePool = stateful.NewScopePool(stateful.FindReferenceVariables(n.Replicas.Expression))
	}
	return kn, nil
}

func (k *K8sAutoscaleNode) runAutoscale([]byte) error {
	k.increaseCount = &expvar.Int{}
	k.decreaseCount = &expvar.Int{}
	errorsCount := &expvar.Int{}
	k.cooldownDropsCount = &expvar.Int{}

	k.statMap.Set(statsK8sIncreaseEventsCount, k.increaseCount)
	k.statMap.Set(statsK8sDecreaseEventsCount, k.decreaseCount)
	k.statMap.Set(statsK8sErrorsCount, errorsCount)
	k.statMap.Set(statsK8sCooldownDropsCount, k.cooldownDropsCount)

	switch k.Wants() {
	case pipeline.StreamEdge:
		for p, ok := k.ins[0].NextPoint(); ok; p, ok = k.ins[0].NextPoint() {
			k.timer.Start()
			if np, err := k.handlePoint(p.Name, p.Group, p.Dimensions, p.Time, p.Fields, p.Tags); err != nil {
				errorsCount.Add(1)
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
					errorsCount.Add(1)
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

type resourceState struct {
	lastIncrease time.Time
	lastDecrease time.Time
	current      int
}

type event struct {
	Namespace string
	Kind      string
	Name      string
	Old       int
	New       int
}

func (k *K8sAutoscaleNode) handlePoint(streamName string, group models.GroupID, dims models.Dimensions, t time.Time, fields models.Fields, tags models.Tags) (models.Point, error) {
	namespace, kind, name, err := k.getResourceFromPoint(tags)
	if err != nil {
		return models.Point{}, err
	}
	state, ok := k.resourceStates[name]
	if !ok {
		// If we haven't seen this resource before, get its state
		scale, err := k.getResource(namespace, kind, name)
		if err != nil {
			return models.Point{}, errors.Wrapf(err, "could not determine initial scale for %s/%s/%s", namespace, kind, name)
		}
		state.current = int(scale.Spec.Replicas)
		k.resourceStates[name] = state
	}

	// Eval the replicas expression
	newReplicas, err := k.evalExpr(state.current, group, k.k.Replicas, k.replicasExprs, k.replicasScopePool, t, fields, tags)
	if err != nil {
		return models.Point{}, errors.Wrap(err, "failed to evaluate the replicas expression")
	}

	// Create the event
	e := event{
		Namespace: namespace,
		Kind:      kind,
		Name:      name,
		Old:       state.current,
		New:       newReplicas,
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
	// Set namespace,kind,resource tags
	if k.k.NamespaceTag != "" {
		newTags[k.k.NamespaceTag] = namespace
	}
	if k.k.KindTag != "" {
		newTags[k.k.KindTag] = kind
	}
	if k.k.ResourceTag != "" {
		newTags[k.k.ResourceTag] = name
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

func (k *K8sAutoscaleNode) getResourceFromPoint(tags models.Tags) (namespace, kind, name string, err error) {
	// Get the name of the resource
	switch {
	case k.k.ResourceName != "":
		name = k.k.ResourceName
	case k.k.ResourceNameTag != "":
		t, ok := tags[k.k.ResourceNameTag]
		if ok {
			name = t
		}
	default:
		return "", "", "", errors.New("expected one of ResourceName, ResourceNameField and ResourceNameTag to be set")
	}
	if name == "" {
		return "", "", "", errors.New("could not determine the name of the resource")
	}
	namespace = k.k.Namespace
	if namespace == "" {
		namespace = client.NamespaceDefault
	}
	kind = k.k.Kind
	return
}

func (k *K8sAutoscaleNode) getResource(namespace, kind, name string) (*client.Scale, error) {
	scales := k.client.Scales(namespace)
	scale, err := scales.Get(kind, name)
	return scale, errors.Wrapf(err, "failed to get the scale for resource %s/%s/%s", namespace, kind, name)
}

func (k *K8sAutoscaleNode) applyEvent(e event) error {
	k.logger.Printf("D! setting scale replicas to %d was %d for %s/%s/%s", e.New, e.Old, e.Namespace, e.Kind, e.Name)
	scales := k.client.Scales(e.Namespace)
	scale, err := k.getResource(e.Namespace, e.Kind, e.Name)
	if err != nil {
		return err
	}
	if scale.Spec.Replicas != int32(e.Old) {
		k.logger.Printf("W! the kubernetes scale spec and Kapacitor's spec do not match for resource %s/%s/%s, did it change externally?", e.Namespace, e.Kind, e.Name)
	}

	scale.Spec.Replicas = int32(e.New)
	if err := scales.Update(e.Kind, scale); err != nil {
		return errors.Wrapf(err, "failed to update the scale for resource %s/%s/%s", e.Namespace, e.Kind, e.Name)
	}
	return nil
}

func (k *K8sAutoscaleNode) evalExpr(
	current int,
	group models.GroupID,
	lambda *ast.LambdaNode,
	expressionsMap map[models.GroupID]stateful.Expression,
	pool stateful.ScopePool,
	t time.Time,
	fields models.Fields,
	tags models.Tags,
) (int, error) {
	expr, ok := expressionsMap[group]
	if !ok {
		var err error
		expr, err = stateful.NewExpression(lambda.Expression)
		if err != nil {
			return 0, err
		}
		expressionsMap[group] = expr
	}
	i, err := k.evalInt(int64(current), expr, pool, t, fields, tags)
	return int(i), err
}

// evalInt - Evaluate a given expression as an int64 against a set of fields and tags.
// The CurrentField is also set on the scope if not empty.
func (k *K8sAutoscaleNode) evalInt(current int64, se stateful.Expression, scopePool stateful.ScopePool, now time.Time, fields models.Fields, tags models.Tags) (int64, error) {
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
	return i, nil
}
