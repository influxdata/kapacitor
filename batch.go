package kapacitor

import (
	"fmt"
	"net/url"
	"time"

	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

type BatchNode struct {
	node
	b       *pipeline.BatchNode
	query   *influxql.SelectStatement
	ticker  *time.Ticker
	startTL *influxql.TimeLiteral
	stopTL  *influxql.TimeLiteral
	conf    client.Config
}

func newBatchNode(et *ExecutingTask, n *pipeline.BatchNode) (*BatchNode, error) {
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

	// Parse and validate query
	stmt, err := influxql.ParseStatement(n.Query)
	if err != nil {
		return nil, err
	}
	var ok bool
	bn.query, ok = stmt.(*influxql.SelectStatement)
	if !ok {
		return nil, fmt.Errorf("query is not a select statement %q", n.Query)
	}

	// Add in dimensions
	hasTime := false
	for _, d := range n.Dimensions {
		switch dim := d.(type) {
		case time.Duration:
			if hasTime {
				return nil, fmt.Errorf("groupBy cannot have more than one time dimension")
			}
			// Add time dimension
			hasTime = true
			bn.query.Dimensions = append(bn.query.Dimensions,
				&influxql.Dimension{
					Expr: &influxql.Call{
						Name: "time",
						Args: []influxql.Expr{
							&influxql.DurationLiteral{
								Val: dim,
							},
						},
					},
				})
		case string:
			bn.query.Dimensions = append(bn.query.Dimensions,
				&influxql.Dimension{
					Expr: &influxql.VarRef{
						Val: dim,
					},
				})
		default:
			return nil, fmt.Errorf("invalid dimension type:%T, must be string or time.Duration", d)
		}
	}

	if !hasTime {
		return nil, fmt.Errorf("groupBy must have a time dimension.")
	}

	// Add in time condition nodes
	bn.startTL = &influxql.TimeLiteral{}
	startExpr := &influxql.BinaryExpr{
		Op:  influxql.GT,
		LHS: &influxql.VarRef{Val: "time"},
		RHS: bn.startTL,
	}

	bn.stopTL = &influxql.TimeLiteral{}
	stopExpr := &influxql.BinaryExpr{
		Op:  influxql.LT,
		LHS: &influxql.VarRef{Val: "time"},
		RHS: bn.stopTL,
	}

	if bn.query.Condition != nil {
		bn.query.Condition = &influxql.BinaryExpr{
			Op:  influxql.AND,
			LHS: bn.query.Condition,
			RHS: &influxql.BinaryExpr{
				Op:  influxql.AND,
				LHS: startExpr,
				RHS: stopExpr,
			},
		}
	} else {
		bn.query.Condition = &influxql.BinaryExpr{
			Op:  influxql.AND,
			LHS: startExpr,
			RHS: stopExpr,
		}
	}

	return bn, nil
}

// Query InfluxDB return Edge with data
func (b *BatchNode) Query(batch BatchCollector) {
	defer batch.Close()

	b.ticker = time.NewTicker(b.b.Period)
	for now := range b.ticker.C {

		// Update times for query
		b.startTL.Val = now.Add(-1 * b.b.Period)
		b.stopTL.Val = now

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
