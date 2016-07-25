package kapacitor

import (
	"log"

	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

const (
	statsFieldsDeleted = "fields_deleted"
	statsTagsDeleted   = "tags_deleted"
)

type DeleteNode struct {
	node
	d *pipeline.DeleteNode

	fieldsDeleted *expvar.Int
	tagsDeleted   *expvar.Int
}

// Create a new  DeleteNode which applies a transformation func to each point in a stream and returns a single point.
func newDeleteNode(et *ExecutingTask, n *pipeline.DeleteNode, l *log.Logger) (*DeleteNode, error) {
	dn := &DeleteNode{
		node: node{Node: n, et: et, logger: l},
		d:    n,
	}
	dn.node.runF = dn.runDelete
	return dn, nil
}

func (e *DeleteNode) runDelete(snapshot []byte) error {
	e.fieldsDeleted = &expvar.Int{}
	e.tagsDeleted = &expvar.Int{}

	e.statMap.Set(statsFieldsDeleted, e.fieldsDeleted)
	e.statMap.Set(statsTagsDeleted, e.tagsDeleted)
	switch e.Provides() {
	case pipeline.StreamEdge:
		for p, ok := e.ins[0].NextPoint(); ok; p, ok = e.ins[0].NextPoint() {
			e.timer.Start()
			p.Fields, p.Tags = e.doDeletes(p.Fields, p.Tags)
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
				b.Points[i].Fields, b.Points[i].Tags = e.doDeletes(b.Points[i].Fields, b.Points[i].Tags)
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

func (d *DeleteNode) doDeletes(fields models.Fields, tags models.Tags) (models.Fields, models.Tags) {
	newFields := fields
	fieldsCopied := false
	for _, field := range d.d.Fields {
		if _, ok := fields[field]; ok {
			if !fieldsCopied {
				newFields = newFields.Copy()
				fieldsCopied = true
			}
			d.fieldsDeleted.Add(1)
			delete(newFields, field)
		}
	}
	newTags := tags
	tagsCopied := false
	for _, tag := range d.d.Tags {
		if _, ok := tags[tag]; ok {
			if !tagsCopied {
				newTags = newTags.Copy()
				tagsCopied = true
			}
			d.tagsDeleted.Add(1)
			delete(newTags, tag)
		}
	}
	return newFields, newTags
}
