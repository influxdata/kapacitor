package kapacitor

import (
	"encoding/json"
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

// Return list of databases and retention policies
// the batcher will query.
func (b *BatchNode) DBRPs() ([]DBRP, error) {
	return b.query.DBRPs()
}

// Query InfluxDB return Edge with data
func (b *BatchNode) Query(batch BatchCollector) {
	defer batch.Close()

	b.ticker = time.NewTicker(b.b.Period)
	for now := range b.ticker.C {

		// Update times for query
		b.query.Start(now.Add(-1 * b.b.Period))
		b.query.Stop(now)

		b.logger.Println("D! starting next batch query:", b.query.String())

		// Connect
		con, err := b.et.tm.InfluxDBService.NewClient()
		if err != nil {
			b.logger.Println("E! " + err.Error())
			break
		}
		q := client.Query{
			Command: b.query.String(),
		}

		// Execute query
		resp, err := con.Query(q)
		if err != nil {
			b.logger.Println("E! " + err.Error())
			return
		}

		if resp.Err != nil {
			b.logger.Println("E! " + resp.Err.Error())
			return
		}

		// Collect batches
		for _, res := range resp.Results {
			if res.Err != nil {
				b.logger.Println("E! " + res.Err.Error())
				return
			}
			for _, series := range res.Series {
				groupID := models.TagsToGroupID(
					models.SortedKeys(series.Tags),
					series.Tags,
				)
				bch := models.Batch{
					Name:  series.Name,
					Group: groupID,
					Tags:  series.Tags,
				}
				bch.Points = make([]models.TimeFields, 0, len(series.Values))
				for _, v := range series.Values {
					var skip bool
					fields := make(models.Fields)
					var t time.Time
					for i, c := range series.Columns {
						if c == "time" {
							tStr, ok := v[i].(string)
							if !ok {
								b.logger.Printf("E! unexpected time value: %v", v[i])
								return
							}
							t, err = time.Parse(time.RFC3339, tStr)
							if err != nil {
								b.logger.Println("E! unexpected time format: " + err.Error())
								return
							}
						} else {
							value := v[i]
							if n, ok := value.(json.Number); ok {
								f, err := n.Float64()
								if err == nil {
									value = f
								}
							}
							if value == nil {
								skip = true
								break
							}
							fields[c] = value
						}
					}
					if !skip {
						bch.Points = append(
							bch.Points,
							models.TimeFields{Time: t, Fields: fields},
						)
					}
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
	for bt, ok := b.ins[0].NextBatch(); ok; bt, ok = b.ins[0].NextBatch() {
		for _, child := range b.outs {
			err := child.CollectBatch(bt)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
