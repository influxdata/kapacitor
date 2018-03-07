package kapacitor

import (
	"fmt"
	"strconv"
	"text/template"
	text "text/template"

	"github.com/influxdata/kapacitor/bufpool"
	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/services/sideload"
	"github.com/pkg/errors"
)

type SideloadNode struct {
	node
	s          *pipeline.SideloadNode
	source     sideload.Source
	orderTmpls []orderTmpl

	order []string

	bufferPool *bufpool.Pool
}

// Create a new SideloadNode which loads fields and tags from external sources.
func newSideloadNode(et *ExecutingTask, n *pipeline.SideloadNode, d NodeDiagnostic) (*SideloadNode, error) {
	sn := &SideloadNode{
		node:       node{Node: n, et: et, diag: d},
		s:          n,
		bufferPool: bufpool.New(),
		order:      make([]string, len(n.OrderList)),
		orderTmpls: make([]orderTmpl, len(n.OrderList)),
	}
	src, err := et.tm.SideloadService.Source(n.Source)
	if err != nil {
		return nil, err
	}
	sn.source = src
	for i, o := range n.OrderList {
		op, err := newOrderTmpl(o, sn.bufferPool)
		if err != nil {
			return nil, err
		}
		sn.orderTmpls[i] = op
	}
	sn.node.runF = sn.runSideload
	sn.node.stopF = sn.stopSideload
	return sn, nil
}

func (n *SideloadNode) runSideload([]byte) error {
	consumer := edge.NewConsumerWithReceiver(
		n.ins[0],
		edge.NewReceiverFromForwardReceiverWithStats(
			n.outs,
			edge.NewTimedForwardReceiver(n.timer, n),
		),
	)
	return consumer.Consume()
}
func (n *SideloadNode) stopSideload() {
	n.source.Close()
}

type orderTmpl struct {
	raw        string
	tmpl       *text.Template
	bufferPool *bufpool.Pool
}

func newOrderTmpl(text string, bp *bufpool.Pool) (orderTmpl, error) {
	t, err := template.New("order").Parse(text)
	if err != nil {
		return orderTmpl{}, err
	}
	return orderTmpl{
		raw:        text,
		tmpl:       t,
		bufferPool: bp,
	}, nil
}

func (t orderTmpl) Path(tags models.Tags) (string, error) {
	buf := t.bufferPool.Get()
	defer t.bufferPool.Put(buf)
	err := t.tmpl.Execute(buf, tags)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (n *SideloadNode) doSideload(p edge.FieldsTagsTimeSetter) {
	for i, o := range n.orderTmpls {
		p, err := o.Path(p.Tags())
		if err != nil {
			n.diag.Error("failed to evaluate order template", err, keyvalue.KV("order", o.raw))
			return
		}
		n.order[i] = p
	}
	if len(n.s.Fields) > 0 {
		fields := p.Fields().Copy()
		for key, dflt := range n.s.Fields {
			value := n.source.Lookup(n.order, key)
			if value == nil {
				// Use default
				fields[key] = dflt
			} else {
				v, err := convertType(value, dflt)
				if err != nil {
					n.diag.Error("failed to load key", err, keyvalue.KV("key", key), keyvalue.KV("expected", fmt.Sprintf("%T", dflt)), keyvalue.KV("got", fmt.Sprintf("%T", value)))
					fields[key] = dflt
				} else {
					fields[key] = v
				}
			}
		}
		p.SetFields(fields)
	}
	if len(n.s.Tags) > 0 {
		tags := p.Tags().Copy()
		for key, dflt := range n.s.Tags {
			value := n.source.Lookup(n.order, key)
			if value == nil {
				tags[key] = dflt
			} else {
				v, err := convertType(value, dflt)
				if err != nil {
					n.diag.Error("failed to load key", err, keyvalue.KV("key", key), keyvalue.KV("expected", "string"), keyvalue.KV("got", fmt.Sprintf("%T", value)))
					tags[key] = dflt
				} else {
					tags[key] = v.(string)
				}
			}
		}
		p.SetTags(tags)
	}
}

func (n *SideloadNode) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	begin = begin.ShallowCopy()
	return begin, nil
}

func (n *SideloadNode) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	bp = bp.ShallowCopy()
	n.doSideload(bp)
	return bp, nil
}

func (n *SideloadNode) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return end, nil
}

func (n *SideloadNode) Point(p edge.PointMessage) (edge.Message, error) {
	p = p.ShallowCopy()
	n.doSideload(p)
	return p, nil
}

func (n *SideloadNode) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (n *SideloadNode) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (n *SideloadNode) Done() {}

func convertType(src, dflt interface{}) (interface{}, error) {
	switch dflt.(type) {
	case int64:
		switch src := src.(type) {
		case int64:
			return src, nil
		case float64:
			i := int64(src)
			if float64(i) == src {
				return i, nil
			}
		case string:
			i, err := strconv.Atoi(src)
			if err != nil {
				return nil, errors.Wrap(err, "cannot convert string to int64")
			}
			return i, nil
		}
	case float64:
		switch src := src.(type) {
		case int64:
			return float64(src), nil
		case float64:
			return src, nil
		case string:
			f, err := strconv.ParseFloat(src, 64)
			if err != nil {
				return nil, errors.Wrap(err, "cannot convert string to float64")
			}
			return f, nil
		}
	case bool:
		switch src := src.(type) {
		case bool:
			return src, nil
		case string:
			b, err := strconv.ParseBool(src)
			if err != nil {
				return nil, errors.Wrap(err, "cannot convert string to bool")
			}
			return b, nil
		}
	case string:
		switch src := src.(type) {
		case int64:
			return strconv.FormatInt(src, 10), nil
		case float64:
			return strconv.FormatFloat(src, 'f', -1, 64), nil
		case bool:
			return strconv.FormatBool(src), nil
		case string:
			return src, nil
		}
	}
	return nil, fmt.Errorf("cannot convert value of type %T to type %T", src, dflt)
}
