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
	statsK8sIncreaseCount = "increase_count"
	statsK8sDecreaseCount = "decrease_count"
	statsK8sErrorsCount   = "errors_count"
)

type K8sAutoscaleNode struct {
	node
	k *pipeline.K8sAutoscaleNode

	client client.Client

	increaseExprs     map[models.GroupID]stateful.Expression
	increaseScopePool stateful.ScopePool

	decreaseExprs     map[models.GroupID]stateful.Expression
	decreaseScopePool stateful.ScopePool

	setExprs     map[models.GroupID]stateful.Expression
	setScopePool stateful.ScopePool

	resourceStates map[string]resourceState

	increaseCount *expvar.Int
	decreaseCount *expvar.Int
	errorsCount   *expvar.Int

	min int
	max int
}

// Create a new  K8sAutoscaleNode which caches the most recent item and exposes it over the HTTP API.
func newK8sAutoscaleNode(et *ExecutingTask, n *pipeline.K8sAutoscaleNode, l *log.Logger) (*K8sAutoscaleNode, error) {
	kn := &K8sAutoscaleNode{
		node:           node{Node: n, et: et, logger: l},
		k:              n,
		resourceStates: make(map[string]resourceState),
		min:            int(n.Min),
		max:            int(n.Max),
	}
	if kn.min < 1 {
		return nil, fmt.Errorf("minimum count must be >= 1, got %d", kn.min)
	}
	kn.node.runF = kn.runAutoscale
	// Initialize the lambda expressions
	if n.Increase != nil {
		kn.increaseExprs = make(map[models.GroupID]stateful.Expression)
		kn.increaseScopePool = stateful.NewScopePool(stateful.FindReferenceVariables(n.Increase.Expression))
	}

	if n.Decrease != nil {
		kn.decreaseExprs = make(map[models.GroupID]stateful.Expression)
		kn.decreaseScopePool = stateful.NewScopePool(stateful.FindReferenceVariables(n.Decrease.Expression))
	}

	if n.Set != nil {
		kn.setExprs = make(map[models.GroupID]stateful.Expression)
		kn.setScopePool = stateful.NewScopePool(stateful.FindReferenceVariables(n.Set.Expression))
	}
	if et.tm.K8sService == nil {
		return nil, errors.New("cannot use the k8sAutoscale node, the kubernetes service is not enabled")
	}
	kn.client = et.tm.K8sService.Client()
	return kn, nil
}

func (k *K8sAutoscaleNode) runAutoscale([]byte) error {
	k.increaseCount = &expvar.Int{}
	k.decreaseCount = &expvar.Int{}
	k.errorsCount = &expvar.Int{}

	k.statMap.Set(statsK8sIncreaseCount, k.increaseCount)
	k.statMap.Set(statsK8sDecreaseCount, k.decreaseCount)
	k.statMap.Set(statsK8sErrorsCount, k.errorsCount)

	switch k.Wants() {
	case pipeline.StreamEdge:
		for p, ok := k.ins[0].NextPoint(); ok; p, ok = k.ins[0].NextPoint() {
			k.timer.Start()
			if np, err := k.handlePoint(p.Name, p.Group, p.Dimensions, p.Time, p.Fields, p.Tags); err != nil {
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

func (k *K8sAutoscaleNode) handlePoint(name string, group models.GroupID, dims models.Dimensions, t time.Time, fields models.Fields, tags models.Tags) (models.Point, error) {
	namespace, kind, name, err := k.getResourceFromPoint(fields, tags)
	if err != nil {
		return models.Point{}, err
	}
	state, ok := k.resourceStates[name]
	if !ok {
		// If we haven't seen this resource before get its state
		scale, err := k.getResource(namespace, kind, name)
		if err != nil {
			return models.Point{}, errors.Wrapf(err, "could not determine initial scale for %s/%s/%s", namespace, kind, name)
		}
		state.current = int(scale.Spec.Replicas)
		k.resourceStates[name] = state
	}

	// Eval all expressions
	var increase, decrease, set int
	if k.k.Increase != nil {
		increase, err = k.evalExpr(state.current, group, k.k.Increase, k.increaseExprs, k.increaseScopePool, t, fields, tags)
		if err != nil {
			return models.Point{}, errors.Wrap(err, "failed to evaluate increase expression")
		}
	}
	if k.k.Decrease != nil {
		decrease, err = k.evalExpr(state.current, group, k.k.Decrease, k.decreaseExprs, k.decreaseScopePool, t, fields, tags)
		if err != nil {
			return models.Point{}, errors.Wrap(err, "failed to evaluate decrease expression")
		}
	}
	useSet := false
	if k.k.Set != nil {
		useSet = true
		set, err = k.evalExpr(state.current, group, k.k.Set, k.setExprs, k.setScopePool, t, fields, tags)
		if err != nil {
			return models.Point{}, errors.Wrap(err, "failed to evaluate set expression")
		}
	}
	// Check that the result is sane
	if increase != 0 && decrease != 0 {
		return models.Point{}, fmt.Errorf("cannot increase and decrease in the same event, got increase %d and decrease %d", increase, decrease)
	}
	if useSet && (increase != 0 || decrease != 0) {
		return models.Point{}, fmt.Errorf("cannot set and increase/decrease in the same event, got set %d, increase %d and decrease %d", set, increase, decrease)
	}

	// transform set into an increase/decrease operation
	if useSet {
		if set > state.current {
			increase = set - state.current
		} else if set < state.current {
			decrease = state.current - set
		}
	}
	change := increase
	change -= decrease

	// Create the event
	e := event{
		Namespace: namespace,
		Kind:      kind,
		Name:      name,
		Old:       state.current,
		New:       state.current + change,
	}
	// Check bounds
	if e.New > k.max {
		e.New = k.max
	}
	if e.New < k.min {
		e.New = k.min
	}

	// Check something changed
	if e.New == e.Old {
		// Nothing to do
		return models.Point{}, nil
	}

	// Update local copy of state
	state.current = e.New
	var counter *expvar.Int
	switch {
	case increase != 0:
		if !t.After(state.lastIncrease.Add(k.k.IncreaseCooldown)) {
			// Nothing to do
			return models.Point{}, nil
		}
		state.lastIncrease = t
		counter = k.increaseCount
	case decrease != 0:
		if !t.After(state.lastDecrease.Add(k.k.DecreaseCooldown)) {
			// Nothing to do
			return models.Point{}, nil
		}
		state.lastDecrease = t
		counter = k.decreaseCount
	default:
		// Nothing to do
		return models.Point{}, nil
	}
	// We have a real event to apply
	if err := k.applyEvent(e); err != nil {
		k.errorsCount.Add(1)
		return models.Point{}, errors.Wrap(err, "failed to apply scaling event")
	}
	// Only save the updated state if we were successful
	k.resourceStates[name] = state
	// Count event
	counter.Add(1)

	p := models.Point{
		Name:       name,
		Time:       t,
		Group:      group,
		Dimensions: dims,
		Tags:       tags,
		Fields: models.Fields{
			"old": int64(e.Old),
			"new": int64(e.New),
		},
	}
	return p, nil
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

func (k *K8sAutoscaleNode) getResourceFromPoint(fields models.Fields, tags models.Tags) (namespace, kind, name string, err error) {
	// Get the name of the resource
	switch {
	case k.k.ResourceName != "":
		name = k.k.ResourceName
	case k.k.ResourceNameField != "":
		f, ok := fields[k.k.ResourceNameField]
		if ok {
			if n, ok := f.(string); ok {
				name = n
			}
		}
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
	return scale, errors.Wrapf(err, "failed to get the current scale for resource %s/%s/%s", namespace, kind, name)
}

func (k *K8sAutoscaleNode) applyEvent(e event) error {
	k.logger.Printf("D! setting scale replicas to %d was %d for %s/%s/%s", e.New, e.Old, e.Namespace, e.Kind, e.Name)
	scales := k.client.Scales(e.Namespace)
	scale, err := k.getResource(e.Namespace, e.Kind, e.Name)
	if err != nil {
		return errors.Wrap(err, "failed to apply e")
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

// evalInt - Evaluate a given expression as an int64 against a set of fields and tags.
// The CurrentField is also set on the scope if not empty.
func (k *K8sAutoscaleNode) evalInt(current int64, se stateful.Expression, scopePool stateful.ScopePool, now time.Time, fields models.Fields, tags models.Tags) (int64, error) {
	vars := scopePool.Get()
	if k.k.CurrentField != "" {
		vars.Set(k.k.CurrentField, current)
	}
	defer scopePool.Put(vars)
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
