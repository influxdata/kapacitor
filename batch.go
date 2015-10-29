package kapacitor

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/gorhill/cronexpr"
	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

type BatchNode struct {
	node
	b      *pipeline.BatchNode
	query  *Query
	ticker ticker
	wg     sync.WaitGroup
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

	if n.Every != 0 && n.Cron != "" {
		return nil, errors.New("must not set both 'every' and 'cron' properties")
	}
	switch {
	case n.Every != 0:
		bn.ticker = newTimeTicker(n.Every)
	case n.Cron != "":
		var err error
		bn.ticker, err = newCronTicker(n.Cron)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("must define one of 'every' or 'cron'")
	}

	return bn, nil
}

// Return list of databases and retention policies
// the batcher will query.
func (b *BatchNode) DBRPs() ([]DBRP, error) {
	return b.query.DBRPs()
}

func (b *BatchNode) Start(batch BatchCollector) {
	b.wg.Add(1)
	go b.doQuery(batch)
}

func (b *BatchNode) NextQueries(start time.Time, num int) []string {
	now := time.Now()
	// Crons are sensitive to timezones.
	start = start.Local()
	queries := make([]string, 0, num)
	for i := 0; i < num || num == 0; i++ {
		start = b.ticker.Next(start)
		if start.IsZero() {
			break
		}
		b.query.Start(start)
		stop := start.Add(b.b.Period)
		if stop.After(now) {
			break
		}
		b.query.Stop(stop)
		queries = append(queries, b.query.String())

	}
	return queries
}

// Query InfluxDB return Edge with data
func (b *BatchNode) doQuery(batch BatchCollector) {
	defer batch.Close()
	defer b.wg.Done()

	tickC := b.ticker.Start()
	for now := range tickC {

		// Update times for query
		stop := now.Add(-1 * b.b.Offset)
		b.query.Start(stop.Add(-1 * b.b.Period))
		b.query.Stop(stop)

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
	b.wg.Wait()
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

type ticker interface {
	Start() <-chan time.Time
	Stop()
	// Return the next time the ticker will tick
	// after now.
	Next(now time.Time) time.Time
}

type timeTicker struct {
	every  time.Duration
	ticker *time.Ticker
}

func newTimeTicker(every time.Duration) *timeTicker {
	return &timeTicker{every: every}
}

func (t *timeTicker) Start() <-chan time.Time {
	t.ticker = time.NewTicker(t.every)
	return t.ticker.C
}

func (t *timeTicker) Stop() {
	if t.ticker != nil {
		t.ticker.Stop()
	}
}

func (t *timeTicker) Next(now time.Time) time.Time {
	return now.Add(t.every)
}

type cronTicker struct {
	expr    *cronexpr.Expression
	ticker  chan time.Time
	closing chan struct{}
	wg      sync.WaitGroup
}

func newCronTicker(cronExpr string) (*cronTicker, error) {
	expr, err := cronexpr.Parse(cronExpr)
	if err != nil {
		return nil, err
	}
	return &cronTicker{
		expr:    expr,
		ticker:  make(chan time.Time),
		closing: make(chan struct{}),
	}, nil
}

func (c *cronTicker) Start() <-chan time.Time {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			now := time.Now()
			next := c.expr.Next(now)
			diff := next.Sub(now)
			select {
			case <-time.After(diff):
				c.ticker <- next
			case <-c.closing:
				return
			}
		}
	}()
	return c.ticker
}

func (c *cronTicker) Stop() {
	close(c.closing)
	c.wg.Wait()
}

func (c *cronTicker) Next(now time.Time) time.Time {
	return c.expr.Next(now)
}
