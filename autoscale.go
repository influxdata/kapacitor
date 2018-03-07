package kapacitor

import (
	"fmt"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	ec2 "github.com/influxdata/kapacitor/services/ec2/client"
	k8s "github.com/influxdata/kapacitor/services/k8s/client"
	swarm "github.com/influxdata/kapacitor/services/swarm/client"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
	"github.com/pkg/errors"
)

const (
	statsAutoscaleIncreaseEventsCount = "increase_events"
	statsAutoscaleDecreaseEventsCount = "decrease_events"
	statsAutoscaleCooldownDropsCount  = "cooldown_drops"
)

type resourceID interface {
	ID() string
}

type autoscaler interface {
	ResourceIDFromTags(models.Tags) (resourceID, error)
	Replicas(id resourceID) (int, error)
	SetReplicas(id resourceID, replicas int) error
	SetResourceIDOnTags(id resourceID, tags models.Tags)
}

type resourceState struct {
	lastIncrease time.Time
	lastDecrease time.Time
	current      int
}

type event struct {
	ID  resourceID
	Old int
	New int
}

type AutoscaleNode struct {
	node

	a autoscaler

	replicasExpr      stateful.Expression
	replicasScopePool stateful.ScopePool

	resourceStates map[string]resourceState

	increaseCount      *expvar.Int
	decreaseCount      *expvar.Int
	cooldownDropsCount *expvar.Int

	min int
	max int

	increaseCooldown time.Duration
	decreaseCooldown time.Duration

	currentField string
}

// Create a new AutoscaleNode which can trigger autoscale events.
func newAutoscaleNode(
	et *ExecutingTask,
	d NodeDiagnostic,
	n pipeline.Node,
	a autoscaler,
	min,
	max int,
	increaseCooldown,
	decreaseCooldown time.Duration,
	currentField string,
	replicas *ast.LambdaNode,
) (*AutoscaleNode, error) {
	if min < 1 {
		return nil, fmt.Errorf("minimum count must be >= 1, got %d", min)
	}
	// Initialize the replicas lambda expression scope pool
	replicasExpr, err := stateful.NewExpression(replicas.Expression)
	if err != nil {
		return nil, errors.Wrap(err, "invalid replicas expression")
	}
	replicasScopePool := stateful.NewScopePool(ast.FindReferenceVariables(replicas.Expression))
	kn := &AutoscaleNode{
		node:              node{Node: n, et: et, diag: d},
		resourceStates:    make(map[string]resourceState),
		min:               min,
		max:               max,
		increaseCooldown:  increaseCooldown,
		decreaseCooldown:  decreaseCooldown,
		currentField:      currentField,
		a:                 a,
		replicasExpr:      replicasExpr,
		replicasScopePool: replicasScopePool,
	}
	kn.node.runF = kn.runAutoscale
	return kn, nil
}

func (n *AutoscaleNode) runAutoscale([]byte) error {
	n.increaseCount = &expvar.Int{}
	n.decreaseCount = &expvar.Int{}
	n.cooldownDropsCount = &expvar.Int{}

	n.statMap.Set(statsAutoscaleIncreaseEventsCount, n.increaseCount)
	n.statMap.Set(statsAutoscaleDecreaseEventsCount, n.decreaseCount)
	n.statMap.Set(statsAutoscaleCooldownDropsCount, n.cooldownDropsCount)

	consumer := edge.NewGroupedConsumer(
		n.ins[0],
		n,
	)
	n.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())
	return consumer.Consume()
}

func (n *AutoscaleNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	return edge.NewReceiverFromForwardReceiverWithStats(
		n.outs,
		edge.NewTimedForwardReceiver(n.timer, n.newGroup()),
	), nil
}

func (n *AutoscaleNode) newGroup() *autoscaleGroup {
	return &autoscaleGroup{
		n:    n,
		expr: n.replicasExpr.CopyReset(),
	}
}

type autoscaleGroup struct {
	n *AutoscaleNode

	expr stateful.Expression

	begin edge.BeginBatchMessage
}

func (g *autoscaleGroup) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	g.begin = begin
	return nil, nil
}

func (g *autoscaleGroup) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	np, err := g.n.handlePoint(g.begin.Name(), g.begin.Dimensions(), bp, g.expr)
	if err != nil {
		g.n.diag.Error("error batch handling point", err)
	}
	return np, nil
}

func (g *autoscaleGroup) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return nil, nil
}

func (g *autoscaleGroup) Point(p edge.PointMessage) (edge.Message, error) {
	np, err := g.n.handlePoint(p.Name(), p.Dimensions(), p, g.expr)
	if err != nil {
		g.n.diag.Error("error handling point", err)
	}
	return np, nil
}

func (g *autoscaleGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (g *autoscaleGroup) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (g *autoscaleGroup) Done() {}

func (n *AutoscaleNode) handlePoint(streamName string, dims models.Dimensions, p edge.FieldsTagsTimeGetter, expr stateful.Expression) (edge.PointMessage, error) {
	id, err := n.a.ResourceIDFromTags(p.Tags())
	if err != nil {
		return nil, err
	}
	state, ok := n.resourceStates[id.ID()]
	if !ok {
		// If we haven't seen this resource before, get its state
		replicas, err := n.a.Replicas(id)
		if err != nil {
			return nil, errors.Wrapf(err, "could not determine initial scale for %q", id)
		}
		state = resourceState{
			current: replicas,
		}
		n.resourceStates[id.ID()] = state
	}

	// Eval the replicas expression
	newReplicas, err := n.evalExpr(state.current, expr, p)
	if err != nil {
		return nil, errors.Wrap(err, "failed to evaluate the replicas expression")
	}

	// Create the event
	e := event{
		ID:  id,
		Old: state.current,
		New: newReplicas,
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
		if t.Before(state.lastIncrease.Add(n.increaseCooldown)) {
			// Still hot, nothing to do
			n.cooldownDropsCount.Add(1)
			return nil, nil
		}
		state.lastIncrease = t
		counter = n.increaseCount
	case change < 0:
		if t.Before(state.lastDecrease.Add(n.decreaseCooldown)) {
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
	n.resourceStates[id.ID()] = state

	// Count event
	counter.Add(1)

	// Create new tags for the point.
	// Leave room for the namespace,kind, and resource tags.
	newTags := make(models.Tags, len(dims.TagNames)+3)

	// Copy group by tags
	for _, d := range dims.TagNames {
		newTags[d] = p.Tags()[d]
	}
	n.a.SetResourceIDOnTags(id, newTags)

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

func (n *AutoscaleNode) applyEvent(e event) error {
	n.diag.SettingReplicas(e.New, e.Old, e.ID.ID())
	err := n.a.SetReplicas(e.ID, e.New)
	return errors.Wrapf(err, "failed to set new replica count for %q", e.ID)
}

func (n *AutoscaleNode) evalExpr(
	current int,
	expr stateful.Expression,
	p edge.FieldsTagsTimeGetter,
) (int, error) {
	vars := n.replicasScopePool.Get()
	defer n.replicasScopePool.Put(vars)

	// Set the current replicas value on the scope if requested.
	if n.currentField != "" {
		vars.Set(n.currentField, current)
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

////////////////////////////////////
// K8s implementation of Autoscaler

type k8sAutoscaler struct {
	client k8s.Client

	resourceName    string
	resourceNameTag string

	namespaceTag string
	kindTag      string
	nameTag      string

	kind string

	namespace string
}

func newK8sAutoscaleNode(et *ExecutingTask, n *pipeline.K8sAutoscaleNode, d NodeDiagnostic) (*AutoscaleNode, error) {
	client, err := et.tm.K8sService.Client(n.Cluster)
	if err != nil {
		return nil, fmt.Errorf("cannot use the k8sAutoscale node, could not create kubernetes client: %v", err)
	}
	a := &k8sAutoscaler{
		client:          client,
		resourceName:    n.ResourceName,
		resourceNameTag: n.ResourceNameTag,
		namespaceTag:    n.NamespaceTag,
		kindTag:         n.KindTag,
		nameTag:         n.ResourceTag,
		kind:            n.Kind,
		namespace:       n.Namespace,
	}
	return newAutoscaleNode(
		et,
		d,
		n,
		a,
		int(n.Min),
		int(n.Max),
		n.IncreaseCooldown,
		n.DecreaseCooldown,
		n.CurrentField,
		n.Replicas,
	)
}

type k8sResourceID struct {
	Namespace,
	Kind,
	Name string
}

func (id k8sResourceID) ID() string {
	return id.Name
}

func (id k8sResourceID) String() string {
	return fmt.Sprintf("%s/%s/%s", id.Namespace, id.Kind, id.Name)
}

func (a *k8sAutoscaler) ResourceIDFromTags(tags models.Tags) (resourceID, error) {
	// Get the name of the resource
	var name string
	switch {
	case a.resourceName != "":
		name = a.resourceName
	case a.resourceNameTag != "":
		t, ok := tags[a.resourceNameTag]
		if ok {
			name = t
		}
	default:
		return nil, errors.New("expected one of ResourceName or ResourceNameTag to be set")
	}
	if name == "" {
		return nil, errors.New("could not determine the name of the resource")
	}
	namespace := a.namespace
	if namespace == "" {
		namespace = k8s.NamespaceDefault
	}
	return k8sResourceID{
		Namespace: namespace,
		Kind:      a.kind,
		Name:      name,
	}, nil
}

func (a *k8sAutoscaler) getScale(kid k8sResourceID) (*k8s.Scale, error) {
	scales := a.client.Scales(kid.Namespace)
	scale, err := scales.Get(kid.Kind, kid.Name)
	return scale, err
}

func (a *k8sAutoscaler) Replicas(id resourceID) (int, error) {
	kid := id.(k8sResourceID)
	scale, err := a.getScale(kid)
	if err != nil {
		return 0, err
	}
	return int(scale.Spec.Replicas), nil
}

func (a *k8sAutoscaler) SetReplicas(id resourceID, replicas int) error {
	kid := id.(k8sResourceID)
	scale, err := a.getScale(kid)
	if err != nil {
		return err
	}
	scale.Spec.Replicas = int32(replicas)
	scales := a.client.Scales(kid.Namespace)
	if err := scales.Update(kid.Kind, scale); err != nil {
		return err
	}
	return nil
}

func (a *k8sAutoscaler) SetResourceIDOnTags(id resourceID, tags models.Tags) {
	kid := id.(k8sResourceID)
	// Set namespace,kind,resource tags
	if a.namespaceTag != "" {
		tags[a.namespaceTag] = kid.Namespace
	}
	if a.kindTag != "" {
		tags[a.kindTag] = kid.Kind
	}
	if a.nameTag != "" {
		tags[a.nameTag] = kid.Name
	}
}

/////////////////////////////////////////////
// Docker Swarm implementation of Autoscaler

type swarmAutoscaler struct {
	client swarm.Client

	serviceName          string
	serviceNameTag       string
	outputServiceNameTag string
}

func newSwarmAutoscaleNode(et *ExecutingTask, n *pipeline.SwarmAutoscaleNode, d NodeDiagnostic) (*AutoscaleNode, error) {
	client, err := et.tm.SwarmService.Client(n.Cluster)
	if err != nil {
		return nil, fmt.Errorf("cannot use the swarmAutoscale node, could not create swarm client: %v", err)
	}
	outputServiceNameTag := n.OutputServiceNameTag
	if outputServiceNameTag == "" {
		outputServiceNameTag = n.ServiceNameTag
	}
	a := &swarmAutoscaler{
		client:               client,
		serviceName:          n.ServiceName,
		serviceNameTag:       n.ServiceNameTag,
		outputServiceNameTag: outputServiceNameTag,
	}
	return newAutoscaleNode(
		et,
		d,
		n,
		a,
		int(n.Min),
		int(n.Max),
		n.IncreaseCooldown,
		n.DecreaseCooldown,
		n.CurrentField,
		n.Replicas,
	)
}

type swarmResourceID string

func (id swarmResourceID) ID() string {
	return string(id)
}

func (a *swarmAutoscaler) ResourceIDFromTags(tags models.Tags) (resourceID, error) {
	// Get the name of the resource
	var name string
	switch {
	case a.serviceName != "":
		name = a.serviceName
	case a.serviceNameTag != "":
		t, ok := tags[a.serviceNameTag]
		if ok {
			name = t
		}
	default:
		return nil, errors.New("expected one of ServiceName or ServiceNameTag to be set")
	}
	if name == "" {
		return nil, errors.New("could not determine the name of the resource")
	}
	return swarmResourceID(name), nil
}

func (a *swarmAutoscaler) Replicas(id resourceID) (int, error) {
	sid := id.ID()
	service, err := a.client.Service(sid)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get swarm service for %q", id)
	}
	return int(*service.Spec.Mode.Replicated.Replicas), nil

}

func (a *swarmAutoscaler) SetReplicas(id resourceID, replicas int) error {
	sid := id.ID()
	service, err := a.client.Service(sid)
	if err != nil {
		return errors.Wrapf(err, "failed to get swarm service for %q", id)
	}
	*service.Spec.Mode.Replicated.Replicas = uint64(replicas)

	return a.client.UpdateService(service)
}

func (a *swarmAutoscaler) SetResourceIDOnTags(id resourceID, tags models.Tags) {
	if a.outputServiceNameTag != "" {
		tags[a.outputServiceNameTag] = id.ID()
	}
}

/////////////////////////////////////////////
// EC2 implementation of Autoscaler

type ec2Autoscaler struct {
	client ec2.Client

	groupName          string
	groupNameTag       string
	outputGroupNameTag string
}

func newEc2AutoscaleNode(et *ExecutingTask, n *pipeline.Ec2AutoscaleNode, d NodeDiagnostic) (*AutoscaleNode, error) {
	client, err := et.tm.EC2Service.Client(n.Cluster)
	if err != nil {
		return nil, fmt.Errorf("cannot use the EC2Autoscale node, could not create ec2 client: %v", err)
	}
	outputGroupNameTag := n.OutputGroupNameTag
	if outputGroupNameTag == "" {
		outputGroupNameTag = n.GroupNameTag
	}
	a := &ec2Autoscaler{
		client: client,

		groupName:          n.GroupName,
		groupNameTag:       n.GroupNameTag,
		outputGroupNameTag: outputGroupNameTag,
	}
	return newAutoscaleNode(
		et,
		d,
		n,
		a,
		int(n.Min),
		int(n.Max),
		n.IncreaseCooldown,
		n.DecreaseCooldown,
		n.CurrentField,
		n.Replicas,
	)
}

type ec2ResourceID string

func (id ec2ResourceID) ID() string {
	return string(id)
}

func (a *ec2Autoscaler) ResourceIDFromTags(tags models.Tags) (resourceID, error) {
	// Get the name of the resource
	var name string
	switch {
	case a.groupName != "":
		name = a.groupName
	case a.groupNameTag != "":
		t, ok := tags[a.groupNameTag]
		if ok {
			name = t
		}
	default:
		return nil, errors.New("expected one of GroupName or GroupNameTag to be set")
	}
	if name == "" {
		return nil, errors.New("could not determine the name of the resource")
	}
	return swarmResourceID(name), nil
}

func (a *ec2Autoscaler) Replicas(id resourceID) (int, error) {
	sid := id.ID()
	group, err := a.client.Group(sid)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get ec2 autoscaleGroup for %q", id)
	}
	var desiredcapacity int64
	for _, resp := range group.AutoScalingGroups {
		desiredcapacity = *resp.DesiredCapacity
	}
	return int(desiredcapacity), nil

}

func (a *ec2Autoscaler) SetReplicas(id resourceID, replicas int) error {
	sid := id.ID()

	return a.client.UpdateGroup(sid, int64(replicas))
}

func (a *ec2Autoscaler) SetResourceIDOnTags(id resourceID, tags models.Tags) {
	if a.outputGroupNameTag != "" {
		tags[a.outputGroupNameTag] = id.ID()
	}
}
