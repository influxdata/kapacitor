package kapacitor

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorhill/cronexpr"
	client "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

type SourceBatchNode struct {
	node
	s   *pipeline.SourceBatchNode
	idx int
}

func newSourceBatchNode(et *ExecutingTask, n *pipeline.SourceBatchNode, l *log.Logger) (*SourceBatchNode, error) {
	sn := &SourceBatchNode{
		node: node{Node: n, et: et, logger: l},
		s:    n,
	}
	return sn, nil
}

func (s *SourceBatchNode) linkChild(c Node) error {

	// add child
	if s.Provides() != c.Wants() {
		return fmt.Errorf("cannot add child mismatched edges: %s -> %s", s.Provides(), c.Wants())
	}
	s.children = append(s.children, c)

	// add parent
	c.addParent(s)

	return nil
}

func (s *SourceBatchNode) addParentEdge(in *Edge) {
	// Pass edges down to children
	s.children[s.idx].addParentEdge(in)
	s.idx++
}

func (s *SourceBatchNode) start([]byte) {
}

func (s *SourceBatchNode) Wait() error {
	return nil
}

// Return list of databases and retention policies
// the batcher will query.
func (s *SourceBatchNode) DBRPs() ([]DBRP, error) {
	var dbrps []DBRP
	for _, b := range s.children {
		d, err := b.(*BatchNode).DBRPs()
		if err != nil {
			return nil, err
		}
		dbrps = append(dbrps, d...)
	}
	return dbrps, nil
}

func (s *SourceBatchNode) Count() int {
	return len(s.children)
}

func (s *SourceBatchNode) Start() {
	for _, b := range s.children {
		b.(*BatchNode).Start()
	}
}

func (s *SourceBatchNode) Abort() {
	for _, b := range s.children {
		b.(*BatchNode).Abort()
	}
}

func (s *SourceBatchNode) Queries(start, stop time.Time) [][]string {
	queries := make([][]string, len(s.children))
	for i, b := range s.children {
		queries[i] = b.(*BatchNode).Queries(start, stop)
	}
	return queries
}

// Do not add the source batch node to the dot output
// since its not really an edge.
func (s *SourceBatchNode) edot(*bytes.Buffer, bool) {}

func (s *SourceBatchNode) collectedCount() (count int64) {
	for _, child := range s.children {
		count += child.collectedCount()
	}
	return
}

const (
	statsQueryErrors   = "query_errors"
	statsConnectErrors = "connect_errors"
)

type BatchNode struct {
	node
	b        *pipeline.BatchNode
	query    *Query
	ticker   ticker
	queryMu  sync.Mutex
	queryErr chan error
	closing  chan struct{}
	aborting chan struct{}
}

func newBatchNode(et *ExecutingTask, n *pipeline.BatchNode, l *log.Logger) (*BatchNode, error) {
	bn := &BatchNode{
		node:     node{Node: n, et: et, logger: l},
		b:        n,
		closing:  make(chan struct{}),
		aborting: make(chan struct{}),
	}
	bn.node.runF = bn.runBatch
	bn.node.stopF = bn.stopBatch

	// Create query
	q, err := NewQuery(n.QueryStr)
	if err != nil {
		return nil, err
	}
	bn.query = q
	// Add in dimensions
	err = bn.query.Dimensions(n.Dimensions)
	if err != nil {
		return nil, err
	}
	// Set fill
	switch fill := n.Fill.(type) {
	case string:
		switch fill {
		case "null":
			bn.query.Fill(influxql.NullFill, nil)
		case "none":
			bn.query.Fill(influxql.NoFill, nil)
		case "previous":
			bn.query.Fill(influxql.PreviousFill, nil)
		default:
			return nil, fmt.Errorf("unexpected fill option %s", fill)
		}
	case int64, float64:
		bn.query.Fill(influxql.NumberFill, fill)
	}

	// Determine schedule
	if n.Every != 0 && n.Cron != "" {
		return nil, errors.New("must not set both 'every' and 'cron' properties")
	}
	switch {
	case n.Every != 0:
		bn.ticker = newTimeTicker(n.Every, n.AlignFlag)
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

func (b *BatchNode) Start() {
	b.queryMu.Lock()
	defer b.queryMu.Unlock()
	b.queryErr = make(chan error, 1)
	go func() {
		b.queryErr <- b.doQuery()
	}()
}

func (b *BatchNode) Abort() {
	close(b.aborting)
}

func (b *BatchNode) Queries(start, stop time.Time) []string {
	now := time.Now()
	if stop.IsZero() {
		stop = now
	}
	// Crons are sensitive to timezones.
	// Make sure we are using local time.
	start = start.Local()
	queries := make([]string, 0)
	for {
		start = b.ticker.Next(start)
		if start.IsZero() || start.After(stop) {
			break
		}
		b.query.Start(start)
		qstop := start.Add(b.b.Period)
		if qstop.After(now) {
			break
		}
		b.query.Stop(qstop)
		queries = append(queries, b.query.String())
	}
	return queries
}

// Query InfluxDB and collect batches on batch collector.
func (b *BatchNode) doQuery() error {
	defer b.ins[0].Close()

	if b.et.tm.InfluxDBService == nil {
		return errors.New("InfluxDB not configured, cannot query InfluxDB for batch query")
	}

	tickC := b.ticker.Start()
	for {
		select {
		case <-b.closing:
			return nil
		case <-b.aborting:
			return errors.New("batch doQuery aborted")
		case now := <-tickC:
			b.timer.Start()

			// Update times for query
			stop := now.Add(-1 * b.b.Offset)
			b.query.Start(stop.Add(-1 * b.b.Period))
			b.query.Stop(stop)

			b.logger.Println("D! starting next batch query:", b.query.String())

			// Connect
			var con client.Client
			var err error
			if b.b.Cluster != "" {
				con, err = b.et.tm.InfluxDBService.NewNamedClient(b.b.Cluster)
			} else {
				con, err = b.et.tm.InfluxDBService.NewDefaultClient()
			}
			if err != nil {
				b.logger.Println("E! failed to connect to InfluxDB:", err)
				b.timer.Stop()
				b.statMap.Add(statsConnectErrors, 1)
				break
			}
			q := client.Query{
				Command: b.query.String(),
			}

			// Execute query
			resp, err := con.Query(q)
			if err != nil {
				b.logger.Println("E! query failed:", err)
				b.timer.Stop()
				b.statMap.Add(statsQueryErrors, 1)
				break
			}

			if err := resp.Error(); err != nil {
				b.logger.Println("E! query failed:", err)
				b.timer.Stop()
				b.statMap.Add(statsQueryErrors, 1)
				break
			}

			// Collect batches
			for _, res := range resp.Results {
				batches, err := models.ResultToBatches(res)
				if err != nil {
					b.logger.Println("E! failed to understand query result:", err)
					b.statMap.Add(statsQueryErrors, 1)
					continue
				}
				b.timer.Pause()
				for _, bch := range batches {
					err := b.ins[0].CollectBatch(bch)
					if err != nil {
						return err
					}
				}
				b.timer.Resume()
			}
			b.timer.Stop()
		}
	}
}

func (b *BatchNode) runBatch([]byte) error {
	b.statMap.Add(statsQueryErrors, 0)
	b.statMap.Add(statsConnectErrors, 0)
	errC := make(chan error, 1)
	go func() {
		defer func() {
			err := recover()
			if err != nil {
				errC <- fmt.Errorf("%v", err)
			}
		}()
		for bt, ok := b.ins[0].NextBatch(); ok; bt, ok = b.ins[0].NextBatch() {
			for _, child := range b.outs {
				err := child.CollectBatch(bt)
				if err != nil {
					errC <- err
					return
				}
			}
		}
		errC <- nil
	}()
	var queryErr error
	b.queryMu.Lock()
	if b.queryErr != nil {
		b.queryMu.Unlock()
		select {
		case queryErr = <-b.queryErr:
		case <-b.aborting:
			queryErr = errors.New("batch queryErr aborted")
		}
	} else {
		b.queryMu.Unlock()
	}

	var err error
	select {
	case err = <-errC:
	case <-b.aborting:
		err = errors.New("batch run aborted")
	}
	if queryErr != nil {
		return queryErr
	}
	return err
}

func (b *BatchNode) stopBatch() {
	if b.ticker != nil {
		b.ticker.Stop()
	}
	close(b.closing)
}

type ticker interface {
	Start() <-chan time.Time
	Stop()
	// Return the next time the ticker will tick after now.
	Next(now time.Time) time.Time
}

type timeTicker struct {
	every     time.Duration
	alignChan chan time.Time
	stopping  chan struct{}
	ticker    *time.Ticker
	mu        sync.Mutex
	wg        sync.WaitGroup
}

func newTimeTicker(every time.Duration, align bool) *timeTicker {
	t := &timeTicker{
		every: every,
	}
	if align {
		t.alignChan = make(chan time.Time)
		t.stopping = make(chan struct{})
	}
	return t
}

func (t *timeTicker) Start() <-chan time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.alignChan != nil {
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			// Sleep until we are roughly aligned
			now := time.Now()
			next := now.Truncate(t.every).Add(t.every)
			after := time.NewTicker(next.Sub(now))
			select {
			case <-after.C:
				after.Stop()
			case <-t.stopping:
				after.Stop()
				return
			}
			t.ticker = time.NewTicker(t.every)
			// Send first event since we waited for it explicitly
			t.alignChan <- next
			for {
				select {
				case <-t.stopping:
					return
				case now := <-t.ticker.C:
					now = now.Truncate(t.every)
					t.alignChan <- now
				}
			}
		}()
		return t.alignChan
	} else {
		t.ticker = time.NewTicker(t.every)
		return t.ticker.C
	}
}

func (t *timeTicker) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ticker != nil {
		t.ticker.Stop()
	}
	if t.alignChan != nil {
		close(t.stopping)
	}
	t.wg.Wait()
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
