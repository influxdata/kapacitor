package kapacitor

import (
	"log"
	"sort"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick"
)

type GroupByNode struct {
	node
	g             *pipeline.GroupByNode
	dimensions    []string
	allDimensions bool
}

// Create a new GroupByNode which splits the stream dynamically based on the specified dimensions.
func newGroupByNode(et *ExecutingTask, n *pipeline.GroupByNode, l *log.Logger) (*GroupByNode, error) {
	gn := &GroupByNode{
		node: node{Node: n, et: et, logger: l},
		g:    n,
	}
	gn.node.runF = gn.runGroupBy

	gn.allDimensions, gn.dimensions = determineDimensions(n.Dimensions)
	return gn, nil
}

func (g *GroupByNode) runGroupBy([]byte) error {
	switch g.Wants() {
	case pipeline.StreamEdge:
		for pt, ok := g.ins[0].NextPoint(); ok; pt, ok = g.ins[0].NextPoint() {
			pt = setGroupOnPoint(pt, g.allDimensions, g.dimensions)
			for _, child := range g.outs {
				err := child.CollectPoint(pt)
				if err != nil {
					return err
				}
			}
		}
	default:
		panic("not implemented")
	}
	return nil
}

func determineDimensions(dimensions []interface{}) (allDimensions bool, realDimensions []string) {
DIMS:
	for _, dim := range dimensions {
		switch d := dim.(type) {
		case string:
			realDimensions = append(realDimensions, d)
		case *tick.StarNode:
			allDimensions = true
			break DIMS
		}
	}
	sort.Strings(realDimensions)
	return
}

func setGroupOnPoint(p models.Point, allDimensions bool, dimensions []string) models.Point {
	if allDimensions {
		dimensions = models.SortedKeys(p.Tags)
	}
	p.Group = models.TagsToGroupID(dimensions, p.Tags)
	p.Dimensions = dimensions
	return p
}
