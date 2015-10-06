package kapacitor

import (
	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

type InfluxDBOutNode struct {
	node
	i *pipeline.InfluxDBOutNode
}

func newInfluxDBOutNode(et *ExecutingTask, n *pipeline.InfluxDBOutNode) (*InfluxDBOutNode, error) {
	in := &InfluxDBOutNode{
		node: node{Node: n, et: et},
		i:    n,
	}
	in.node.runF = in.runOut
	return in, nil
}

func (i *InfluxDBOutNode) runOut() error {
	switch i.Wants() {
	case pipeline.StreamEdge:
		for p := i.ins[0].NextPoint(); p != nil; p = i.ins[0].NextPoint() {
			err := i.write([]*models.Point{p})
			if err != nil {
				return err
			}
		}
	case pipeline.BatchEdge:
		for w := i.ins[0].NextBatch(); w != nil; w = i.ins[0].NextBatch() {
			err := i.write(w)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (i *InfluxDBOutNode) write(pts []*models.Point) error {

	c, err := i.et.tm.InfluxDBService.NewClient()
	if err != nil {
		return err
	}
	points := make([]client.Point, len(pts))
	for j, p := range pts {
		points[j] = client.Point{
			Measurement: i.i.Measurement,
			Tags:        p.Tags,
			Time:        p.Time,
			Fields:      p.Fields,
		}
	}

	bp := client.BatchPoints{
		Points:           points,
		Database:         i.i.Database,
		RetentionPolicy:  i.i.RetentionPolicy,
		WriteConsistency: i.i.WriteConsistency,
		Tags:             i.i.Tags,
	}
	_, err = c.Write(bp)
	return err
}
