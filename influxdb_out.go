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
		for p, ok := i.ins[0].NextPoint(); ok; p, ok = i.ins[0].NextPoint() {
			batch := models.Batch{
				Name:   p.Name,
				Group:  p.Group,
				Tags:   p.Tags,
				Points: []models.TimeFields{{Time: p.Time, Fields: p.Fields}},
			}
			err := i.write(batch)
			if err != nil {
				return err
			}
		}
	case pipeline.BatchEdge:
		for b, ok := i.ins[0].NextBatch(); ok; b, ok = i.ins[0].NextBatch() {
			err := i.write(b)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (i *InfluxDBOutNode) write(batch models.Batch) error {

	i.logger.Println("D! batch", batch)
	c, err := i.et.tm.InfluxDBService.NewClient()
	if err != nil {
		return err
	}
	points := make([]client.Point, len(batch.Points))
	for j, p := range batch.Points {
		points[j] = client.Point{
			Measurement: i.i.Measurement,
			Tags:        batch.Tags,
			Time:        p.Time,
			Fields:      p.Fields,
			Precision:   i.i.Precision,
		}
	}

	bp := client.BatchPoints{
		Points:           points,
		Database:         i.i.Database,
		RetentionPolicy:  i.i.RetentionPolicy,
		WriteConsistency: i.i.WriteConsistency,
		Tags:             i.i.Tags,
		Precision:        i.i.Precision,
	}
	_, err = c.Write(bp)
	return err
}
