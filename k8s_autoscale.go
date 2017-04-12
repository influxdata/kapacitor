package kapacitor

import (
	"fmt"
	"log"
	"time"

	"github.com/influxdata/kapacitor/edge"
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
	statsK8sCooldownDropsCount  = "cooldown_drops"
)

type K8sAutoscaleNode struct {
	node
	k *pipeline.K8sAutoscaleNode

	client client.Client

	replicasExpr      stateful.Expression
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
	client, err := et.tm.K8sService.Client(n.Cluster)
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
		expr, err := stateful.NewExpression(n.Replicas.Expression)
		if err != nil {
			return nil, err
		}
		kn.replicasExpr = expr
		kn.replicasScopePool = stateful.NewScopePool(ast.FindReferenceVariables(n.Replicas.Expression))
	}
	return kn, nil
}

func (n *K8sAutoscaleNode) runAutoscale([]byte) error {
	n.increaseCount = &expvar.Int{}
	n.decreaseCount = &expvar.Int{}
	n.cooldownDropsCount = &expvar.Int{}

	n.statMap.Set(statsK8sIncreaseEventsCount, n.increaseCount)
	n.statMap.Set(statsK8sDecreaseEventsCount, n.decreaseCount)
	n.statMap.Set(statsK8sCooldownDropsCount, n.cooldownDropsCount)

	consumer := edge.NewGroupedConsumer(
		n.ins[0],
		n,
	)
	n.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())
	return consumer.Consume()
}

func (n *K8sAutoscaleNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	return edge.NewReceiverFromForwardReceiverWithStats(
		n.outs,
		edge.NewTimedForwardReceiver(n.timer, n.newGroup()),
	), nil
}

func (n *K8sAutoscaleNode) newGroup() *k8sAutoscaleGroup {
	return &k8sAutoscaleGroup{
		n:    n,
		expr: n.replicasExpr.CopyReset(),
	}
}

type k8sAutoscaleGroup struct {
	n *K8sAutoscaleNode

	expr stateful.Expression

	begin edge.BeginBatchMessage
}

func (g *k8sAutoscaleGroup) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	g.begin = begin
	return nil, nil
}

func (g *k8sAutoscaleGroup) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	np, err := g.n.handlePoint(g.begin.Name(), g.begin.Dimensions(), bp, g.expr)
	if err != nil {
		g.n.incrementErrorCount()
		g.n.logger.Println("E!", err)
	}
	return np, nil
}

func (g *k8sAutoscaleGroup) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return nil, nil
}

func (g *k8sAutoscaleGroup) Point(p edge.PointMessage) (edge.Message, error) {
	np, err := g.n.handlePoint(p.Name(), p.Dimensions(), p, g.expr)
	if err != nil {
		g.n.incrementErrorCount()
		g.n.logger.Println("E!", err)
	}
	return np, nil
}

func (g *k8sAutoscaleGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (g *k8sAutoscaleGroup) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
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

func (n *K8sAutoscaleNode) handlePoint(streamName string, dims models.Dimensions, p edge.FieldsTagsTimeGetter, expr stateful.Expression) (edge.PointMessage, error) {
	namespace, kind, name, err := n.getResourceFromPoint(p.Tags())
	if err != nil {
		return nil, err
	}
	state, ok := n.resourceStates[name]
	if !ok {
		// If we haven't seen this resource before, get its state
		scale, err := n.getResource(namespace, kind, name)
		if err != nil {
			return nil, errors.Wrapf(err, "could not determine initial scale for %s/%s/%s", namespace, kind, name)
		}
		state.current = int(scale.Spec.Replicas)
		n.resourceStates[name] = state
	}

	// Eval the replicas expression
	newReplicas, err := n.evalExpr(state.current, expr, p)
	if err != nil {
		return nil, errors.Wrap(err, "failed to evaluate the replicas expression")
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
	if n.max > 0 && e.New > n.max {
		e.New = n.max
	}
	if e.New < n.min {
		e.New = n.min
	}

	// Validate something changed
	if e.New == e.Old {
		// Nothing to do
		return nil, nil
	}

	// Update local copy of state
	change := e.New - e.Old
	state.current = e.New

	// Check last change cooldown times
	t := p.Time()
	var counter *expvar.Int
	switch {
	case change > 0:
		if t.Before(state.lastIncrease.Add(n.k.IncreaseCooldown)) {
			// Still hot, nothing to do
			n.cooldownDropsCount.Add(1)
			return nil, nil
		}
		state.lastIncrease = t
		counter = n.increaseCount
	case change < 0:
		if t.Before(state.lastDecrease.Add(n.k.DecreaseCooldown)) {
			// Still hot, nothing to do
			n.cooldownDropsCount.Add(1)
			return nil, nil
		}
		state.lastDecrease = t
		counter = n.decreaseCount
	}

	// We have a valid event to apply
	if err := n.applyEvent(e); err != nil {
		return nil, errors.Wrap(err, "failed to apply scaling event")
	}

	// Only save the updated state if we were successful
	n.resourceStates[name] = state

	// Count event
	counter.Add(1)

	// Create new tags for the point.
	// Leave room for the namespace,kind, and resource tags.
	newTags := make(models.Tags, len(dims.TagNames)+3)

	// Copy group by tags
	for _, d := range dims.TagNames {
		newTags[d] = p.Tags()[d]
	}
	// Set namespace,kind,resource tags
	if n.k.NamespaceTag != "" {
		newTags[n.k.NamespaceTag] = namespace
	}
	if n.k.KindTag != "" {
		newTags[n.k.KindTag] = kind
	}
	if n.k.ResourceTag != "" {
		newTags[n.k.ResourceTag] = name
	}

	// Create point representing the event
	return edge.NewPointMessage(
		streamName, "", "",
		dims,
		models.Fields{
			"old": int64(e.Old),
			"new": int64(e.New),
		},
		newTags,
		t,
	), nil
}

func (n *K8sAutoscaleNode) getResourceFromPoint(tags models.Tags) (namespace, kind, name string, err error) {
	// Get the name of the resource
	switch {
	case n.k.ResourceName != "":
		name = n.k.ResourceName
	case n.k.ResourceNameTag != "":
		t, ok := tags[n.k.ResourceNameTag]
		if ok {
			name = t
		}
	default:
		return "", "", "", errors.New("expected one of ResourceName, ResourceNameField and ResourceNameTag to be set")
	}
	if name == "" {
		return "", "", "", errors.New("could not determine the name of the resource")
	}
	namespace = n.k.Namespace
	if namespace == "" {
		namespace = client.NamespaceDefault
	}
	kind = n.k.Kind
	return
}

func (n *K8sAutoscaleNode) getResource(namespace, kind, name string) (*client.Scale, error) {
	scales := n.client.Scales(namespace)
	scale, err := scales.Get(kind, name)
	return scale, errors.Wrapf(err, "failed to get the scale for resource %s/%s/%s", namespace, kind, name)
}

func (n *K8sAutoscaleNode) applyEvent(e event) error {
	n.logger.Printf("D! setting scale replicas to %d was %d for %s/%s/%s", e.New, e.Old, e.Namespace, e.Kind, e.Name)
	scales := n.client.Scales(e.Namespace)
	scale, err := n.getResource(e.Namespace, e.Kind, e.Name)
	if err != nil {
		return err
	}
	if scale.Spec.Replicas != int32(e.Old) {
		n.logger.Printf("W! the kubernetes scale spec and Kapacitor's spec do not match for resource %s/%s/%s, did it change externally?", e.Namespace, e.Kind, e.Name)
	}

	scale.Spec.Replicas = int32(e.New)
	if err := scales.Update(e.Kind, scale); err != nil {
		return errors.Wrapf(err, "failed to update the scale for resource %s/%s/%s", e.Namespace, e.Kind, e.Name)
	}
	return nil
}

func (n *K8sAutoscaleNode) evalExpr(
	current int,
	expr stateful.Expression,
	p edge.FieldsTagsTimeGetter,
) (int, error) {
	vars := n.replicasScopePool.Get()
	defer n.replicasScopePool.Put(vars)

	// Set the current replicas value on the scope if requested.
	if n.k.CurrentField != "" {
		vars.Set(n.k.CurrentField, current)
	}

	// Fill the scope with the rest of the values
	err := fillScope(vars, n.replicasScopePool.ReferenceVariables(), p)
	if err != nil {
		return 0, err
	}

	i, err := expr.EvalInt(vars)
	if err != nil {
		return 0, err
	}
	return int(i), err
}
