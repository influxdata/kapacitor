package kapacitor

import (
	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

const (
	statsFieldsDefaulted = "fields_defaulted"
	statsTagsDefaulted   = "tags_defaulted"
)

type DefaultNode struct {
	node
	d *pipeline.DefaultNode

	fieldsDefaulted *expvar.Int
	tagsDefaulted   *expvar.Int
}

// Create a new  DefaultNode which applies a transformation func to each point in a stream and returns a single point.
func newDefaultNode(et *ExecutingTask, n *pipeline.DefaultNode, d NodeDiagnostic) (*DefaultNode, error) {
	dn := &DefaultNode{
		node:            node{Node: n, et: et, diag: d},
		d:               n,
		fieldsDefaulted: new(expvar.Int),
		tagsDefaulted:   new(expvar.Int),
	}
	dn.node.runF = dn.runDefault
	return dn, nil
}

func (n *DefaultNode) runDefault(snapshot []byte) error {
	n.statMap.Set(statsFieldsDefaulted, n.fieldsDefaulted)
	n.statMap.Set(statsTagsDefaulted, n.tagsDefaulted)

	consumer := edge.NewConsumerWithReceiver(
		n.ins[0],
		edge.NewReceiverFromForwardReceiverWithStats(
			n.outs,
			edge.NewTimedForwardReceiver(n.timer, n),
		),
	)
	return consumer.Consume()
}

func (n *DefaultNode) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	begin = begin.ShallowCopy()
	_, tags := n.setDefaults(nil, begin.Tags())
	begin.SetTags(tags)
	return begin, nil
}

func (n *DefaultNode) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	bp = bp.ShallowCopy()
	fields, tags := n.setDefaults(bp.Fields(), bp.Tags())
	bp.SetFields(fields)
	bp.SetTags(tags)
	return bp, nil
}

func (n *DefaultNode) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return end, nil
}

func (n *DefaultNode) Point(p edge.PointMessage) (edge.Message, error) {
	p = p.ShallowCopy()
	fields, tags := n.setDefaults(p.Fields(), p.Tags())
	p.SetFields(fields)
	p.SetTags(tags)
	return p, nil
}

func (n *DefaultNode) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (n *DefaultNode) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (n *DefaultNode) Done() {}

func (n *DefaultNode) setDefaults(fields models.Fields, tags models.Tags) (models.Fields, models.Tags) {
	newFields := fields
	fieldsCopied := false
	for field, value := range n.d.Fields {
		if v := fields[field]; v == nil {
			if !fieldsCopied {
				newFields = newFields.Copy()
				fieldsCopied = true
			}
			n.fieldsDefaulted.Add(1)
			newFields[field] = value
		}
	}
	newTags := tags
	tagsCopied := false
	for tag, value := range n.d.Tags {
		if v := tags[tag]; v == "" {
			if !tagsCopied {
				newTags = newTags.Copy()
				tagsCopied = true
			}
			n.tagsDefaulted.Add(1)
			newTags[tag] = value
		}
	}
	return newFields, newTags
}
