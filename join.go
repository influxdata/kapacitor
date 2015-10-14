package kapacitor

import (
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

type JoinNode struct {
	node
	j *pipeline.JoinNode
}

// Create a new  JoinNode, which takes pairs from parent streams combines them into a single point.
func newJoinNode(et *ExecutingTask, n *pipeline.JoinNode) (*JoinNode, error) {
	jn := &JoinNode{
		j:    n,
		node: node{Node: n, et: et},
	}
	jn.node.runF = jn.runJoin
	return jn, nil
}

func (j *JoinNode) runJoin() error {

	switch j.Wants() {
	case pipeline.StreamEdge:

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
				Name:   j.j.Rename,
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
	}

	return nil
}
