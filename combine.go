package kapacitor

import (
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/stateful"
)

type CombineNode struct {
	node
	c *pipeline.CombineNode

	expressions        []stateful.Expression
	expressionsByGroup map[models.GroupID][]stateful.Expression
	scopePools         []stateful.ScopePool

	combination combination
}

// Create a new CombineNode, which combines a stream with itself dynamically.
func newCombineNode(et *ExecutingTask, n *pipeline.CombineNode, l *log.Logger) (*CombineNode, error) {
	cn := &CombineNode{
		c:                  n,
		node:               node{Node: n, et: et, logger: l},
		expressionsByGroup: make(map[models.GroupID][]stateful.Expression),
		combination:        combination{max: n.Max},
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
		cn.scopePools[i] = stateful.NewScopePool(stateful.FindReferenceVariables(lambda.Expression))
	}
	cn.node.runF = cn.runCombine
	return cn, nil
}

type buffer struct {
	Time       time.Time
	Name       string
	Group      models.GroupID
	Dimensions models.Dimensions
	Points     []rawPoint
}

type timeList []time.Time

func (t timeList) Len() int           { return len(t) }
func (t timeList) Less(i, j int) bool { return t[i].Before(t[j]) }
func (t timeList) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }

func (n *CombineNode) runCombine([]byte) error {
	switch n.Wants() {
	case pipeline.StreamEdge:
		buffers := make(map[models.GroupID]*buffer)
		for p, ok := n.ins[0].NextPoint(); ok; p, ok = n.ins[0].NextPoint() {
			n.timer.Start()
			t := p.Time.Round(n.c.Tolerance)
			currentBuf, ok := buffers[p.Group]
			if !ok {
				currentBuf = &buffer{
					Time:       t,
					Name:       p.Name,
					Group:      p.Group,
					Dimensions: p.Dimensions,
				}
				buffers[p.Group] = currentBuf
			}
			rp := rawPoint{
				Time:   t,
				Fields: p.Fields,
				Tags:   p.Tags,
			}
			if t.Equal(currentBuf.Time) {
				currentBuf.Points = append(currentBuf.Points, rp)
			} else {
				if err := n.combineBuffer(currentBuf); err != nil {
					return err
				}
				currentBuf.Time = t
				currentBuf.Name = p.Name
				currentBuf.Group = p.Group
				currentBuf.Dimensions = p.Dimensions
				currentBuf.Points = currentBuf.Points[0:1]
				currentBuf.Points[0] = rp
			}
			n.timer.Stop()
		}
	case pipeline.BatchEdge:
		allBuffers := make(map[models.GroupID]map[time.Time]*buffer)
		groupTimes := make(map[models.GroupID]time.Time)
		for b, ok := n.ins[0].NextBatch(); ok; b, ok = n.ins[0].NextBatch() {
			n.timer.Start()
			t := b.TMax.Round(n.c.Tolerance)
			buffers, ok := allBuffers[b.Group]
			if !ok {
				buffers = make(map[time.Time]*buffer)
				allBuffers[b.Group] = buffers
				groupTimes[b.Group] = t
			}
			groupTime := groupTimes[b.Group]
			if !t.Equal(groupTime) {
				// Set new groupTime
				groupTimes[b.Group] = t
				// Combine/Emit all old buffers
				times := make(timeList, 0, len(buffers))
				for t := range buffers {
					times = append(times, t)
				}
				sort.Sort(times)
				for _, t := range times {
					if err := n.combineBuffer(buffers[t]); err != nil {
						return err
					}
					delete(buffers, t)
				}
			}
			for _, p := range b.Points {
				t := p.Time.Round(n.c.Tolerance)
				currentBuf, ok := buffers[t]
				if !ok {
					currentBuf = &buffer{
						Time:       t,
						Name:       b.Name,
						Group:      b.Group,
						Dimensions: b.PointDimensions(),
					}
					buffers[t] = currentBuf
				}
				currentBuf.Points = append(currentBuf.Points, rawPoint{
					Time:   t,
					Fields: p.Fields,
					Tags:   p.Tags,
				})
			}
			n.timer.Stop()
		}
	}
	return nil
}

// Simple container for point data.
type rawPoint struct {
	Time   time.Time
	Fields models.Fields
	Tags   models.Tags
}

// Combine a set of points into all their combinations.
func (n *CombineNode) combineBuffer(buf *buffer) error {
	if len(buf.Points) == 0 {
		return nil
	}
	l := len(n.expressions)
	expressions, ok := n.expressionsByGroup[buf.Group]
	if !ok {
		expressions = make([]stateful.Expression, l)
		for i, expr := range n.expressions {
			expressions[i] = expr.CopyReset()
		}
		n.expressionsByGroup[buf.Group] = expressions
	}

	// Compute matching result for all points
	matches := make([]map[int]bool, l)
	for i := 0; i < l; i++ {
		matches[i] = make(map[int]bool, len(buf.Points))
	}
	for idx, p := range buf.Points {
		for i := range expressions {
			matched, err := EvalPredicate(expressions[i], n.scopePools[i], p.Time, p.Fields, p.Tags)
			if err != nil {
				n.logger.Println("E! evaluating lambda expression:", err)
			}
			matches[i][idx] = matched
		}
	}

	p := models.Point{
		Name:       buf.Name,
		Group:      buf.Group,
		Dimensions: buf.Dimensions,
	}
	dimensions := p.Dimensions.ToSet()
	set := make([]rawPoint, l)
	return n.combination.Do(len(buf.Points), l, func(indices []int) error {
		valid := true
		for s := 0; s < l; s++ {
			found := false
			for i := range indices {
				if matches[s][indices[i]] {
					set[s] = buf.Points[indices[i]]
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
			rp := n.merge(set, dimensions)

			p.Time = rp.Time.Round(n.c.Tolerance)
			p.Fields = rp.Fields
			p.Tags = rp.Tags

			n.timer.Pause()
			for _, out := range n.outs {
				err := out.CollectPoint(p)
				if err != nil {
					return err
				}
			}
			n.timer.Resume()
		}
		return nil
	})
}

// Merge a set of points into a single point.
func (n *CombineNode) merge(points []rawPoint, dimensions map[string]bool) rawPoint {
	fields := make(models.Fields, len(points[0].Fields)*len(points))
	tags := make(models.Tags, len(points[0].Tags)*len(points))

	for i, p := range points {
		for field, value := range p.Fields {
			fields[n.c.Names[i]+n.c.Delimiter+field] = value
		}
		for tag, value := range p.Tags {
			if !dimensions[tag] {
				tags[n.c.Names[i]+n.c.Delimiter+tag] = value
			} else {
				tags[tag] = value
			}
		}
	}

	return rawPoint{
		Time:   points[0].Time,
		Fields: fields,
		Tags:   tags,
	}
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
