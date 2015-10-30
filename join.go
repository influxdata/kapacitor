package kapacitor

import (
	"fmt"
	"strings"

	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

type JoinNode struct {
	node
	j *pipeline.JoinNode
}

// Create a new  JoinNode, which takes pairs from parent streams combines them into a single point.
func newJoinNode(et *ExecutingTask, n *pipeline.JoinNode) (*JoinNode, error) {
	for _, name := range n.Names {
		if len(name) == 0 {
			return nil, fmt.Errorf("must provide a prefix name for the join node, see .as() property method")
		}
		if strings.ContainsRune(name, '.') {
			return nil, fmt.Errorf("cannot use name %s as field prefix, it contains a '.' character", name)
		}
	}
	if n.Names[0] == n.Names[1] {
		return nil, fmt.Errorf("cannot use the same prefix name see .as() property method")
	}
	jn := &JoinNode{
		j:    n,
		node: node{Node: n, et: et},
	}
	jn.node.runF = jn.runJoin
	return jn, nil
}

func (j *JoinNode) runJoin() (err error) {
	rename := j.j.StreamName
	if rename == "" {
		rename = j.parents[1].Name()
	}
	switch j.Wants() {
	case pipeline.StreamEdge:
		err = j.joinStreams(rename)
	case pipeline.BatchEdge:
		err = j.joinBatches(rename)
	}

	return
}

func (j *JoinNode) joinStreams(rename string) error {
	groups := []map[models.GroupID]*models.Point{
		make(map[models.GroupID]*models.Point),
		make(map[models.GroupID]*models.Point),
	}
	n := 0
	m := 1

	empty := []bool{false, false}

	for !empty[0] || !empty[1] {

		pn, ok := j.ins[n].NextPoint()
		if !ok {
			empty[n] = true
			n, m = m, n
			continue
		}

		pm := groups[m][pn.Group]
		if pm == nil {
			groups[n][pn.Group] = &pn
			n, m = m, n
			continue
		}
		groups[m][pn.Group] = nil

		fields := make(map[string]interface{}, len(pn.Fields)+len(pm.Fields))
		for k, v := range pn.Fields {
			fields[j.j.Names[n]+"."+k] = v
		}
		for k, v := range pm.Fields {
			fields[j.j.Names[m]+"."+k] = v
		}
		p := models.Point{
			Name:   rename,
			Group:  pn.Group,
			Tags:   pn.Tags,
			Fields: fields,
			Time:   pn.Time,
		}

		for _, out := range j.outs {
			err := out.CollectPoint(p)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (j *JoinNode) joinBatches(rename string) error {
	groups := []map[models.GroupID]*models.Batch{
		make(map[models.GroupID]*models.Batch),
		make(map[models.GroupID]*models.Batch),
	}
	n := 0
	m := 1

	empty := []bool{false, false}

	for !empty[0] || !empty[1] {

		bn, ok := j.ins[n].NextBatch()
		if !ok {
			empty[n] = true
			n, m = m, n
			continue
		}

		bm := groups[m][bn.Group]
		if bm == nil {
			groups[n][bn.Group] = &bn
			n, m = m, n
			continue
		}
		groups[m][bn.Group] = nil

		if len(bn.Points) != len(bm.Points) {
			return fmt.Errorf("batches are different lengths cannot join. Use groupBy time and fill to ensure batches match")
		}
		// Rename fields
		newPoints := make([]models.TimeFields, len(bn.Points))
		for i, pn := range bn.Points {
			pm := bm.Points[i]
			if pn.Time.Equal(pm.Time) {
				fields := make(map[string]interface{}, len(pn.Fields)+len(pm.Fields))
				for k, v := range pn.Fields {
					fields[j.j.Names[n]+"."+k] = v
				}
				for k, v := range pm.Fields {
					fields[j.j.Names[m]+"."+k] = v
				}
				newPoints[i] = models.TimeFields{
					Time:   pn.Time,
					Fields: fields,
				}
			}
		}
		b := models.Batch{
			Name:   rename,
			Group:  bn.Group,
			Tags:   bn.Tags,
			Points: newPoints,
		}

		for _, out := range j.outs {
			err := out.CollectBatch(b)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
