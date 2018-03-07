package kapacitor

import (
	"github.com/influxdata/kapacitor/edge"
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

	tags map[string]bool
}

// Create a new  DeleteNode which applies a transformation func to each point in a stream and returns a single point.
func newDeleteNode(et *ExecutingTask, n *pipeline.DeleteNode, d NodeDiagnostic) (*DeleteNode, error) {
	tags := make(map[string]bool)
	for _, tag := range n.Tags {
		tags[tag] = true
	}

	dn := &DeleteNode{
		node:          node{Node: n, et: et, diag: d},
		d:             n,
		fieldsDeleted: new(expvar.Int),
		tagsDeleted:   new(expvar.Int),
		tags:          tags,
	}
	dn.node.runF = dn.runDelete
	return dn, nil
}

func (n *DeleteNode) runDelete(snapshot []byte) error {
	n.statMap.Set(statsFieldsDeleted, n.fieldsDeleted)
	n.statMap.Set(statsTagsDeleted, n.tagsDeleted)
	consumer := edge.NewConsumerWithReceiver(
		n.ins[0],
		edge.NewReceiverFromForwardReceiverWithStats(
			n.outs,
			edge.NewTimedForwardReceiver(n.timer, n),
		),
	)
	return consumer.Consume()
}

func (n *DeleteNode) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	begin = begin.ShallowCopy()
	_, tags := n.doDeletes(nil, begin.Tags())
	begin.SetTags(tags)
	return begin, nil
}

func (n *DeleteNode) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	bp = bp.ShallowCopy()
	fields, tags := n.doDeletes(bp.Fields(), bp.Tags())
	bp.SetFields(fields)
	bp.SetTags(tags)
	return bp, nil
}

func (n *DeleteNode) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return end, nil
}

func (n *DeleteNode) Point(p edge.PointMessage) (edge.Message, error) {
	p = p.ShallowCopy()
	fields, tags := n.doDeletes(p.Fields(), p.Tags())
	p.SetFields(fields)
	p.SetTags(tags)
	dims := p.Dimensions()
	if n.checkForDeletedDimension(dims) {
		p.SetDimensions(n.deleteDimensions(dims))
	}
	return p, nil
}

func (n *DeleteNode) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (n *DeleteNode) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (n *DeleteNode) Done() {}

// checkForDeletedDimension checks if we deleted a group by dimension
func (n *DeleteNode) checkForDeletedDimension(dimensions models.Dimensions) bool {
	for _, dim := range dimensions.TagNames {
		if n.tags[dim] {
			return true
		}
	}
	return false
}

func (n *DeleteNode) deleteDimensions(dims models.Dimensions) models.Dimensions {
	newTagNames := make([]string, 0, len(dims.TagNames)-1)
	for _, dim := range dims.TagNames {
		if !n.tags[dim] {
			newTagNames = append(newTagNames, dim)
		}
	}
	return models.Dimensions{
		TagNames: newTagNames,
		ByName:   dims.ByName,
	}
}

func (n *DeleteNode) doDeletes(fields models.Fields, tags models.Tags) (models.Fields, models.Tags) {
	newFields := fields
	fieldsCopied := false
	for _, field := range n.d.Fields {
		if _, ok := fields[field]; ok {
			if !fieldsCopied {
				newFields = newFields.Copy()
				fieldsCopied = true
			}
			n.fieldsDeleted.Add(1)
			delete(newFields, field)
		}
	}
	newTags := tags
	tagsCopied := false
	for _, tag := range n.d.Tags {
		if _, ok := tags[tag]; ok {
			if !tagsCopied {
				newTags = newTags.Copy()
				tagsCopied = true
			}
			n.tagsDeleted.Add(1)
			delete(newTags, tag)
		}
	}
	return newFields, newTags
}
