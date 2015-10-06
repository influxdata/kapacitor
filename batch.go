package kapacitor

import (
	"time"

	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

type BatchNode struct {
	node
	b      *pipeline.BatchNode
	query  *Query
	ticker *time.Ticker
}

func newBatchNode(et *ExecutingTask, n *pipeline.BatchNode) (*BatchNode, error) {
	bn := &BatchNode{
		node: node{Node: n, et: et},
		b:    n,
	}
	bn.node.runF = bn.runBatch
	bn.node.stopF = bn.stopBatch

	q, err := NewQuery(n.Query)
	if err != nil {
		return nil, err
	}
	bn.query = q
	err = bn.query.Dimensions(n.Dimensions)
	if err != nil {
		return nil, err
	}

	return bn, nil
}

// Query InfluxDB return Edge with data
func (b *BatchNode) Query(batch BatchCollector) {
	defer batch.Close()

	b.ticker = time.NewTicker(b.b.Period)
	for now := range b.ticker.C {

		// Update times for query
		b.query.Start(now.Add(-1 * b.b.Period))
		b.query.Stop(now)

		b.l.Println("D@starting next batch query:", b.query.String())

		// Connect
		con, err := b.et.tm.InfluxDBService.NewClient()
		if err != nil {
			b.l.Println("E@" + err.Error())
			break
		}
		q := client.Query{
			Command: b.query.String(),
		}

		// Execute query
		resp, err := con.Query(q)
		if err != nil {
			b.l.Println("E@" + err.Error())
			return
		}

		if resp.Err != nil {
			b.l.Println("E@" + resp.Err.Error())
			return
		}

		// Collect batches
		for _, res := range resp.Results {
			if res.Err != nil {
				b.l.Println("E@" + res.Err.Error())
				return
			}
			for _, series := range res.Series {
				bch := make([]*models.Point, len(series.Values))
				groupID := models.TagsToGroupID(
					models.SortedKeys(series.Tags),
					series.Tags,
				)
				for i, v := range series.Values {
					fields := make(map[string]interface{})
					var t time.Time
					for i, c := range series.Columns {
						if c == "time" {
							t, err = time.Parse(time.RFC3339, v[i].(string))
							if err != nil {
								b.l.Println("E@" + err.Error())
								return
							}
						} else {
							fields[c] = v[i]
						}
					}
					bch[i] = models.NewPoint(
						series.Name,
						groupID,
						series.Tags,
						fields,
						t,
					)
				}
				batch.CollectBatch(bch)
			}
		}
	}
}

func (b *BatchNode) stopBatch() {
	if b.ticker != nil {
		b.ticker.Stop()
	}
}

func (b *BatchNode) runBatch() error {
	for bt := b.ins[0].NextBatch(); bt != nil; bt = b.ins[0].NextBatch() {
		for _, child := range b.outs {
			err := child.CollectBatch(bt)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
