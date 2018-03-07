package kapacitor

import (
	"fmt"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

type CombineNode struct {
	node
	c *pipeline.CombineNode

	expressions []stateful.Expression
	scopePools  []stateful.ScopePool

	combination combination
}

// Create a new CombineNode, which combines a stream with itself dynamically.
func newCombineNode(et *ExecutingTask, n *pipeline.CombineNode, d NodeDiagnostic) (*CombineNode, error) {
	cn := &CombineNode{
		c:           n,
		node:        node{Node: n, et: et, diag: d},
		combination: combination{max: n.Max},
	}

	// Create stateful expressions
	cn.expressions = make([]stateful.Expression, len(n.Lambdas))
	cn.scopePools = make([]stateful.ScopePool, len(n.Lambdas))
	for i, lambda := range n.Lambdas {
		statefulExpr, err := stateful.NewExpression(lambda.Expression)
		if err != nil {
			return nil, fmt.Errorf("Failed to compile %v expression: %v", i, err)
		}
		cn.expressions[i] = statefulExpr
		cn.scopePools[i] = stateful.NewScopePool(ast.FindReferenceVariables(lambda.Expression))
	}
	cn.node.runF = cn.runCombine
	return cn, nil
}

func (n *CombineNode) runCombine([]byte) error {
	consumer := edge.NewGroupedConsumer(
		n.ins[0],
		n,
	)
	n.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())
	return consumer.Consume()
}

func (n *CombineNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	expressions := make([]stateful.Expression, len(n.expressions))
	for i, expr := range n.expressions {
		expressions[i] = expr.CopyReset()
	}
	return &combineBuffer{
		n:           n,
		time:        first.Time(),
		name:        first.Name(),
		groupInfo:   group,
		expressions: expressions,
		c:           n.combination,
	}, nil
}

type combineBuffer struct {
	n           *CombineNode
	time        time.Time
	name        string
	groupInfo   edge.GroupInfo
	points      []edge.FieldsTagsTimeSetter
	expressions []stateful.Expression
	c           combination

	begin edge.BeginBatchMessage
}

func (b *combineBuffer) BeginBatch(begin edge.BeginBatchMessage) error {
	b.n.timer.Start()
	defer b.n.timer.Stop()

	b.name = begin.Name()
	b.time = time.Time{}
	if s := begin.SizeHint(); s > cap(b.points) {
		b.points = make([]edge.FieldsTagsTimeSetter, 0, s)
	}
	return nil
}

func (b *combineBuffer) BatchPoint(bp edge.BatchPointMessage) error {
	b.n.timer.Start()
	defer b.n.timer.Stop()
	bp = bp.ShallowCopy()
	return b.addPoint(bp)
}

func (b *combineBuffer) EndBatch(end edge.EndBatchMessage) error {
	b.n.timer.Start()
	defer b.n.timer.Stop()
	if err := b.combine(); err != nil {
		return err
	}
	b.points = b.points[0:0]
	return nil
}

func (b *combineBuffer) Point(p edge.PointMessage) error {
	b.n.timer.Start()
	defer b.n.timer.Stop()
	p = p.ShallowCopy()
	return b.addPoint(p)
}

func (b *combineBuffer) addPoint(p edge.FieldsTagsTimeSetter) error {
	t := p.Time().Round(b.n.c.Tolerance)
	p.SetTime(t)
	if t.Equal(b.time) {
		b.points = append(b.points, p)
	} else {
		if err := b.combine(); err != nil {
			return err
		}
		b.time = t
		b.points = b.points[0:1]
		b.points[0] = p
	}
	return nil
}

func (b *combineBuffer) Barrier(barrier edge.BarrierMessage) error {
	return edge.Forward(b.n.outs, barrier)
}
func (b *combineBuffer) DeleteGroup(d edge.DeleteGroupMessage) error {
	return edge.Forward(b.n.outs, d)
}
func (b *combineBuffer) Done() {}

// Combine a set of points into all their combinations.
func (b *combineBuffer) combine() error {
	if len(b.points) == 0 {
		return nil
	}

	l := len(b.expressions)

	// Compute matching result for all points
	matches := make([]map[int]bool, l)
	for i := 0; i < l; i++ {
		matches[i] = make(map[int]bool, len(b.points))
	}
	for idx, p := range b.points {
		for i := range b.expressions {
			matched, err := EvalPredicate(b.expressions[i], b.n.scopePools[i], p)
			if err != nil {
				b.n.diag.Error("error evaluating lambda expression", err)
			}
			matches[i][idx] = matched
		}
	}

	p := edge.NewPointMessage(
		b.name, "", "",
		b.groupInfo.Dimensions,
		nil,
		nil,
		time.Time{},
	)

	dimensions := p.Dimensions().ToSet()
	set := make([]edge.FieldsTagsTimeSetter, l)
	return b.c.Do(len(b.points), l, func(indices []int) error {
		valid := true
		for s := 0; s < l; s++ {
			found := false
			for i := range indices {
				if matches[s][indices[i]] {
					set[s] = b.points[indices[i]]
					indices = append(indices[0:i], indices[i+1:]...)
					found = true
					break
				}
			}
			if !found {
				valid = false
				break
			}
		}
		if valid {
			fields, tags, t := b.merge(set, dimensions)

			np := p.ShallowCopy()
			np.SetFields(fields)
			np.SetTags(tags)
			np.SetTime(t.Round(b.n.c.Tolerance))

			b.n.timer.Pause()
			err := edge.Forward(b.n.outs, np)
			b.n.timer.Resume()
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// Merge a set of points into a single point.
func (b *combineBuffer) merge(points []edge.FieldsTagsTimeSetter, dimensions map[string]bool) (models.Fields, models.Tags, time.Time) {
	fields := make(models.Fields, len(points[0].Fields())*len(points))
	tags := make(models.Tags, len(points[0].Tags())*len(points))

	for i, p := range points {
		for field, value := range p.Fields() {
			fields[b.n.c.Names[i]+b.n.c.Delimiter+field] = value
		}
		for tag, value := range p.Tags() {
			if !dimensions[tag] {
				tags[b.n.c.Names[i]+b.n.c.Delimiter+tag] = value
			} else {
				tags[tag] = value
			}
		}
	}

	return fields, tags, points[0].Time()
}

// Type for performing actions on a set of combinations.
type combination struct {
	max int64
}

// Do action for each combination, based on combinatorial logic n choose k.
// If n choose k > max an error is returned
func (c combination) Do(n, k int, f func(indices []int) error) error {
	if count := c.Count(int64(n), int64(k)); count > c.max {
		return fmt.Errorf("refusing to perform combination as total combinations %d exceeds max combinations %d", count, c.max)
	} else if count == -1 {
		// Nothing to do
		return nil
	}

	indices := make([]int, k)
	indicesCopy := make([]int, k)
	for i := 0; i < k; i++ {
		indices[i] = i
	}
	copy(indicesCopy, indices)
	if err := f(indicesCopy); err != nil {
		return err
	}
	for {
		i := k - 1
		for ; i >= 0; i-- {
			if indices[i] != i+n-k {
				break
			}
		}
		if i == -1 {
			return nil
		}
		indices[i]++
		for j := i + 1; j < k; j++ {
			indices[j] = indices[j-1] + 1
		}
		copy(indicesCopy, indices)
		if err := f(indicesCopy); err != nil {
			return err
		}
	}
}

// Count the number of possible combinations of n choose k.
func (c combination) Count(n, k int64) int64 {
	if n < k {
		return -1
	}
	count := int64(1)
	for i := int64(0); i < k; i++ {
		count = (count * (n - i)) / (i + 1)
	}
	return count
}
