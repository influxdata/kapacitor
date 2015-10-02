package kapacitor

import (
	"fmt"
	"net/url"
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
	conf   client.Config
}

func newBatchNode(et *ExecutingTask, n *pipeline.BatchNode) (*BatchNode, error) {
	fmt.Println("new batch node ")
	bn := &BatchNode{
		node: node{Node: n, et: et},
		b:    n,
	}
	bn.node.runF = bn.runBatch
	bn.node.stopF = bn.stopBatch

	u, _ := url.Parse("http://localhost:8086")
	bn.conf = client.Config{
		URL: *u,
	}

	q, err := NewQuery(n.Query)
	if err != nil {
		return nil, err
	}
	bn.query = q
	err = bn.query.Dimensions(n.Dimensions)
	if err != nil {
		return nil, err
	}

	fmt.Println("batch node created")

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

		// Connect
		con, err := client.NewClient(b.conf)
		if err != nil {
			fmt.Println(err)
			break
		}
		q := client.Query{
			Command: b.query.String(),
		}

		// Execute query
		resp, err := con.Query(q)
		if err != nil {
			fmt.Println(err)
			return
		}

		if resp.Err != nil {
			fmt.Println(resp.Err)
			return
		}

		// Collect batches
		for _, res := range resp.Results {
			if res.Err != nil {
				fmt.Println(res.Err)
				return
			}
			for _, series := range res.Series {
				b := make([]*models.Point, len(series.Values))
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
								fmt.Println(err)
								return
							}
						} else {
							fields[c] = v[i]
						}
					}
					b[i] = models.NewPoint(
						series.Name,
						groupID,
						series.Tags,
						fields,
						t,
					)
				}
				batch.CollectBatch(b)
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
