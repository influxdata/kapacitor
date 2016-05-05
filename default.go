package kapacitor

import (
	"log"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

type DefaultNode struct {
	node
	d *pipeline.DefaultNode
}

// Create a new  DefaultNode which applies a transformation func to each point in a stream and returns a single point.
func newDefaultNode(et *ExecutingTask, n *pipeline.DefaultNode, l *log.Logger) (*DefaultNode, error) {
	dn := &DefaultNode{
		node: node{Node: n, et: et, logger: l},
		d:    n,
	}
	dn.node.runF = dn.runDefault
	return dn, nil
}

func (e *DefaultNode) runDefault(snapshot []byte) error {
	switch e.Provides() {
	case pipeline.StreamEdge:
		for p, ok := e.ins[0].NextPoint(); ok; p, ok = e.ins[0].NextPoint() {
			e.timer.Start()
			p.Fields, p.Tags = e.setDefaults(p.Fields, p.Tags)
			e.timer.Stop()
			for _, child := range e.outs {
				err := child.CollectPoint(p)
				if err != nil {
					return err
				}
			}
		}
	case pipeline.BatchEdge:
		for b, ok := e.ins[0].NextBatch(); ok; b, ok = e.ins[0].NextBatch() {
			e.timer.Start()
			for i := range b.Points {
				b.Points[i].Fields, b.Points[i].Tags = e.setDefaults(b.Points[i].Fields, b.Points[i].Tags)
			}
			e.timer.Stop()
			for _, child := range e.outs {
				err := child.CollectBatch(b)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (d *DefaultNode) setDefaults(fields models.Fields, tags models.Tags) (models.Fields, models.Tags) {
	newFields := fields
	fieldsCopied := false
	for field, value := range d.d.Fields {
		if _, ok := fields[field]; !ok {
			if !fieldsCopied {
				newFields = newFields.Copy()
				fieldsCopied = true
			}
			newFields[field] = value
		}
	}
	newTags := tags
	tagsCopied := false
	for tag, value := range d.d.Tags {
		if _, ok := tags[tag]; !ok {
			if !tagsCopied {
				newTags = newTags.Copy()
				tagsCopied = true
			}
			newTags[tag] = value
		}
	}
	return newFields, newTags
}
