package kapacitor

import (
	"log"
	"sort"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

type GroupByNode struct {
	node
	g             *pipeline.GroupByNode
	dimensions    []string
	allDimensions bool
	byName        bool
}

// Create a new GroupByNode which splits the stream dynamically based on the specified dimensions.
func newGroupByNode(et *ExecutingTask, n *pipeline.GroupByNode, l *log.Logger) (*GroupByNode, error) {
	gn := &GroupByNode{
		node:   node{Node: n, et: et, logger: l},
		g:      n,
		byName: n.ByMeasurementFlag,
	}
	gn.node.runF = gn.runGroupBy

	gn.allDimensions, gn.dimensions = determineDimensions(n.Dimensions)
	return gn, nil
}

func (g *GroupByNode) runGroupBy([]byte) error {
	dims := models.Dimensions{
		ByName: g.g.ByMeasurementFlag,
	}
	switch g.Wants() {
	case pipeline.StreamEdge:
		dims.TagNames = g.dimensions
		for pt, ok := g.ins[0].NextPoint(); ok; pt, ok = g.ins[0].NextPoint() {
			g.timer.Start()
			pt = setGroupOnPoint(pt, g.allDimensions, dims)
			g.timer.Stop()
			for _, child := range g.outs {
				err := child.CollectPoint(pt)
				if err != nil {
					return err
				}
			}
		}
	default:
		var lastTime time.Time
		groups := make(map[models.GroupID]*models.Batch)
		for b, ok := g.ins[0].NextBatch(); ok; b, ok = g.ins[0].NextBatch() {
			g.timer.Start()
			if !b.TMax.Equal(lastTime) {
				lastTime = b.TMax
				// Emit all groups
				for id, group := range groups {
					for _, child := range g.outs {
						err := child.CollectBatch(*group)
						if err != nil {
							return err
						}
					}
					// Remove from groups
					delete(groups, id)
				}
			}
			for _, p := range b.Points {
				if g.allDimensions {
					dims.TagNames = models.SortedKeys(p.Tags)
				} else {
					dims.TagNames = g.dimensions
				}
				groupID := models.ToGroupID(b.Name, p.Tags, dims)
				group, ok := groups[groupID]
				if !ok {
					tags := make(map[string]string, len(dims.TagNames))
					for _, dim := range dims.TagNames {
						tags[dim] = p.Tags[dim]
					}
					group = &models.Batch{
						Name:   b.Name,
						Group:  groupID,
						TMax:   b.TMax,
						ByName: b.ByName,
						Tags:   tags,
					}
					groups[groupID] = group
				}
				group.Points = append(group.Points, p)
			}
			g.timer.Stop()
		}
	}
	return nil
}

func determineDimensions(dimensions []interface{}) (allDimensions bool, realDimensions []string) {
	for _, dim := range dimensions {
		switch d := dim.(type) {
		case string:
			realDimensions = append(realDimensions, d)
		case *ast.StarNode:
			allDimensions = true
		}
	}
	sort.Strings(realDimensions)
	return
}

func setGroupOnPoint(p models.Point, allDimensions bool, dimensions models.Dimensions) models.Point {
	if allDimensions {
		dimensions.TagNames = models.SortedKeys(p.Tags)
	}
	p.Group = models.ToGroupID(p.Name, p.Tags, dimensions)
	p.Dimensions = dimensions
	return p
}
