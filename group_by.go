package kapacitor

import (
	"sort"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

type GroupByNode struct {
	node
	g *pipeline.GroupByNode

	byName   bool
	tagNames []string

	begin      edge.BeginBatchMessage
	dimensions models.Dimensions

	allDimensions bool

	mu       sync.RWMutex
	lastTime time.Time
	groups   map[models.GroupID]edge.BufferedBatchMessage
}

// Create a new GroupByNode which splits the stream dynamically based on the specified dimensions.
func newGroupByNode(et *ExecutingTask, n *pipeline.GroupByNode, d NodeDiagnostic) (*GroupByNode, error) {
	gn := &GroupByNode{
		node:   node{Node: n, et: et, diag: d},
		g:      n,
		groups: make(map[models.GroupID]edge.BufferedBatchMessage),
	}
	gn.node.runF = gn.runGroupBy

	gn.allDimensions, gn.tagNames = determineTagNames(n.Dimensions, n.ExcludedDimensions)
	gn.byName = n.ByMeasurementFlag
	return gn, nil
}

func (n *GroupByNode) runGroupBy([]byte) error {
	valueF := func() int64 {
		n.mu.RLock()
		l := len(n.groups)
		n.mu.RUnlock()
		return int64(l)
	}
	n.statMap.Set(statCardinalityGauge, expvar.NewIntFuncGauge(valueF))

	consumer := edge.NewConsumerWithReceiver(
		n.ins[0],
		n,
	)
	return consumer.Consume()
}

func (n *GroupByNode) Point(p edge.PointMessage) error {
	p = p.ShallowCopy()
	n.timer.Start()
	dims := p.Dimensions()
	dims.ByName = dims.ByName || n.byName
	dims.TagNames = computeTagNames(p.Tags(), n.allDimensions, n.tagNames, n.g.ExcludedDimensions)
	p.SetDimensions(dims)
	n.timer.Stop()
	if err := edge.Forward(n.outs, p); err != nil {
		return err
	}
	return nil
}

func (n *GroupByNode) BeginBatch(begin edge.BeginBatchMessage) error {
	n.timer.Start()
	defer n.timer.Stop()

	n.emit(begin.Time())

	n.begin = begin
	n.dimensions = begin.Dimensions()
	n.dimensions.ByName = n.dimensions.ByName || n.byName

	return nil
}

func (n *GroupByNode) BatchPoint(bp edge.BatchPointMessage) error {
	n.timer.Start()
	defer n.timer.Stop()

	n.dimensions.TagNames = computeTagNames(bp.Tags(), n.allDimensions, n.tagNames, n.g.ExcludedDimensions)
	groupID := models.ToGroupID(n.begin.Name(), bp.Tags(), n.dimensions)
	group, ok := n.groups[groupID]
	if !ok {
		// Create new begin message
		newBegin := n.begin.ShallowCopy()
		newBegin.SetTagsAndDimensions(bp.Tags(), n.dimensions)

		// Create buffer for group batch
		group = edge.NewBufferedBatchMessage(
			newBegin,
			make([]edge.BatchPointMessage, 0, newBegin.SizeHint()),
			edge.NewEndBatchMessage(),
		)
		n.mu.Lock()
		n.groups[groupID] = group
		n.mu.Unlock()
	}
	group.SetPoints(append(group.Points(), bp))

	return nil
}

func (n *GroupByNode) EndBatch(end edge.EndBatchMessage) error {
	return nil
}

func (n *GroupByNode) Barrier(b edge.BarrierMessage) error {
	n.timer.Start()
	err := n.emit(b.Time())
	n.timer.Stop()
	if err != nil {
		return err
	}
	return edge.Forward(n.outs, b)
}
func (n *GroupByNode) DeleteGroup(d edge.DeleteGroupMessage) error {
	return edge.Forward(n.outs, d)
}
func (n *GroupByNode) Done() {}

// emit sends all groups before time t to children nodes.
// The node timer must be started when calling this method.
func (n *GroupByNode) emit(t time.Time) error {
	// TODO: ensure this time comparison works with barrier messages
	if !t.Equal(n.lastTime) {
		n.lastTime = t
		// Emit all groups
		for id, group := range n.groups {
			// Update SizeHint since we know the final point count
			group.Begin().SetSizeHint(len(group.Points()))
			// Sort points since we didn't guarantee insertion order was sorted
			sort.Sort(edge.BatchPointMessages(group.Points()))
			// Send group batch to all children
			n.timer.Pause()
			if err := edge.Forward(n.outs, group); err != nil {
				return err
			}
			n.timer.Resume()
			n.mu.Lock()
			// Remove from group
			delete(n.groups, id)
			n.mu.Unlock()
		}
	}
	return nil
}

func determineTagNames(dimensions []interface{}, excluded []string) (allDimensions bool, realDimensions []string) {
	for _, dim := range dimensions {
		switch d := dim.(type) {
		case string:
			realDimensions = append(realDimensions, d)
		case *ast.StarNode:
			allDimensions = true
		}
	}
	sort.Strings(realDimensions)
	realDimensions = filterExcludedTagNames(realDimensions, excluded)
	return
}

func filterExcludedTagNames(tagNames, excluded []string) []string {
	filtered := tagNames[0:0]
	for _, t := range tagNames {
		found := false
		for _, x := range excluded {
			if x == t {
				found = true
				break
			}
		}
		if !found {
			filtered = append(filtered, t)
		}
	}
	return filtered
}

func computeTagNames(tags models.Tags, allDimensions bool, tagNames, excluded []string) []string {
	if allDimensions {
		return filterExcludedTagNames(models.SortedKeys(tags), excluded)
	}
	return tagNames
}
