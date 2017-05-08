package kapacitor

import (
	"fmt"
	"log"

	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

const (
	statsKapacitorLoopbackPointsWritten = "points_written"
)

type KapacitorLoopbackNode struct {
	node
	k *pipeline.KapacitorLoopbackNode

	pointsWritten *expvar.Int
}

func newKapacitorLoopbackNode(et *ExecutingTask, n *pipeline.KapacitorLoopbackNode, l *log.Logger) (*KapacitorLoopbackNode, error) {
	kn := &KapacitorLoopbackNode{
		node: node{Node: n, et: et, logger: l},
		k:    n,
	}
	kn.node.runF = kn.runOut
	// Check that a loop has not been created within this task
	for _, dbrp := range et.Task.DBRPs {
		if dbrp.Database == n.Database && dbrp.RetentionPolicy == n.RetentionPolicy {
			return nil, fmt.Errorf("loop detected on dbrp: %v", dbrp)
		}
	}
	return kn, nil
}

func (k *KapacitorLoopbackNode) runOut([]byte) error {
	k.pointsWritten = &expvar.Int{}

	k.statMap.Set(statsInfluxDBPointsWritten, k.pointsWritten)

	switch k.Wants() {
	case pipeline.StreamEdge:
		for p, ok := k.ins[0].NextPoint(); ok; p, ok = k.ins[0].NextPoint() {
			k.timer.Start()
			if k.k.Database != "" {
				p.Database = k.k.Database
			}
			if k.k.RetentionPolicy != "" {
				p.RetentionPolicy = k.k.RetentionPolicy
			}
			if k.k.Measurement != "" {
				p.Name = k.k.Measurement
			}
			if len(k.k.Tags) > 0 {
				p.Tags = p.Tags.Copy()
				for k, v := range k.k.Tags {
					p.Tags[k] = v
				}
			}
			err := k.et.tm.WriteKapacitorPoint(p)
			if err != nil {
				k.incrementErrorCount()
				k.logger.Println("E! failed to write point over loopback")
			} else {
				k.pointsWritten.Add(1)
			}
			k.timer.Stop()
		}
	case pipeline.BatchEdge:
		for b, ok := k.ins[0].NextBatch(); ok; b, ok = k.ins[0].NextBatch() {
			k.timer.Start()
			if k.k.Measurement != "" {
				b.Name = k.k.Measurement
			}
			written := int64(0)
			for _, bp := range b.Points {
				tags := bp.Tags
				if len(k.k.Tags) > 0 {
					tags = bp.Tags.Copy()
					for k, v := range k.k.Tags {
						tags[k] = v
					}
				}
				p := models.Point{
					Database:        k.k.Database,
					RetentionPolicy: k.k.RetentionPolicy,
					Name:            b.Name,
					Tags:            tags,
					Fields:          bp.Fields,
					Time:            bp.Time,
				}
				err := k.et.tm.WriteKapacitorPoint(p)
				if err != nil {
					k.incrementErrorCount()
					k.logger.Println("E! failed to write point over loopback")
				} else {
					written++
				}
			}
			k.pointsWritten.Add(written)
			k.timer.Stop()
		}
	}
	return nil
}
