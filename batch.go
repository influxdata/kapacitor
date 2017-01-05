package kapacitor

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorhill/cronexpr"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/influxdb"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/pkg/errors"
)

const (
	statsQueryErrors    = "query_errors"
	statsBatchesQueried = "batches_queried"
	statsPointsQueried  = "points_queried"
)

type BatchNode struct {
	node
	s   *pipeline.BatchNode
	idx int
}

func newBatchNode(et *ExecutingTask, n *pipeline.BatchNode, l *log.Logger) (*BatchNode, error) {
	sn := &BatchNode{
		node: node{Node: n, et: et, logger: l},
		s:    n,
	}
	return sn, nil
}

func (s *BatchNode) linkChild(c Node) error {

	// add child
	if s.Provides() != c.Wants() {
		return fmt.Errorf("cannot add child mismatched edges: %s -> %s", s.Provides(), c.Wants())
	}
	s.children = append(s.children, c)

	// add parent
	c.addParent(s)

	return nil
}

func (s *BatchNode) addParentEdge(in *Edge) {
	// Pass edges down to children
	s.children[s.idx].addParentEdge(in)
	s.idx++
}

func (s *BatchNode) start([]byte) {
}

func (s *BatchNode) Wait() error {
	return nil
}

// Return list of databases and retention policies
// the batcher will query.
func (s *BatchNode) DBRPs() ([]DBRP, error) {
	var dbrps []DBRP
	for _, b := range s.children {
		d, err := b.(*QueryNode).DBRPs()
		if err != nil {
			return nil, err
		}
		dbrps = append(dbrps, d...)
	}
	return dbrps, nil
}

func (s *BatchNode) Count() int {
	return len(s.children)
}

func (s *BatchNode) Start() {
	for _, b := range s.children {
		b.(*QueryNode).Start()
	}
}

func (s *BatchNode) Abort() {
	for _, b := range s.children {
		b.(*QueryNode).Abort()
	}
}

type BatchQueries struct {
	Queries            []*Query
	Cluster            string
	GroupByMeasurement bool
}

func (s *BatchNode) Queries(start, stop time.Time) ([]BatchQueries, error) {
	queries := make([]BatchQueries, len(s.children))
	for i, b := range s.children {
		qn := b.(*QueryNode)
		qs, err := qn.Queries(start, stop)
		if err != nil {
			return nil, err
		}
		queries[i] = BatchQueries{
			Queries:            qs,
			Cluster:            qn.Cluster(),
			GroupByMeasurement: qn.GroupByMeasurement(),
		}
	}
	return queries, nil
}

// Do not add the source batch node to the dot output
// since its not really an edge.
func (s *BatchNode) edot(*bytes.Buffer, bool) {}

func (s *BatchNode) collectedCount() (count int64) {
	for _, child := range s.children {
		count += child.collectedCount()
	}
	return
}

type QueryNode struct {
	node
	b        *pipeline.QueryNode
	query    *Query
	ticker   ticker
	queryMu  sync.Mutex
	queryErr chan error
	closing  chan struct{}
	aborting chan struct{}

	queryErrors    *expvar.Int
	batchesQueried *expvar.Int
	pointsQueried  *expvar.Int
	byName         bool
}

func newQueryNode(et *ExecutingTask, n *pipeline.QueryNode, l *log.Logger) (*QueryNode, error) {
	bn := &QueryNode{
		node:     node{Node: n, et: et, logger: l},
		b:        n,
		closing:  make(chan struct{}),
		aborting: make(chan struct{}),
		byName:   n.GroupByMeasurementFlag,
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
	// Set offset alignment
	if n.AlignGroupFlag {
		bn.query.AlignGroup()
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

func (b *QueryNode) GroupByMeasurement() bool {
	return b.byName
}

// Return list of databases and retention policies
// the batcher will query.
func (b *QueryNode) DBRPs() ([]DBRP, error) {
	return b.query.DBRPs()
}

func (b *QueryNode) Start() {
	b.queryMu.Lock()
	defer b.queryMu.Unlock()
	b.queryErr = make(chan error, 1)
	go func() {
		b.queryErr <- b.doQuery()
	}()
}

func (b *QueryNode) Abort() {
	close(b.aborting)
}

func (b *QueryNode) Cluster() string {
	return b.b.Cluster
}

func (b *QueryNode) Queries(start, stop time.Time) ([]*Query, error) {
	now := time.Now()
	if stop.IsZero() {
		stop = now
	}
	// Crons are sensitive to timezones.
	// Make sure we are using local time.
	current := start.Local()
	queries := make([]*Query, 0)
	for {
		current = b.ticker.Next(current)
		if current.IsZero() || current.After(stop) {
			break
		}
		qstop := current.Add(-1 * b.b.Offset)
		if qstop.After(now) {
			break
		}

		q, err := b.query.Clone()
		if err != nil {
			return nil, err
		}
		q.SetStartTime(qstop.Add(-1 * b.b.Period))
		q.SetStopTime(qstop)
		queries = append(queries, q)
	}
	return queries, nil
}

// Query InfluxDB and collect batches on batch collector.
func (b *QueryNode) doQuery() error {
	defer b.ins[0].Close()
	b.queryErrors = &expvar.Int{}
	b.batchesQueried = &expvar.Int{}
	b.pointsQueried = &expvar.Int{}

	b.statMap.Set(statsQueryErrors, b.queryErrors)
	b.statMap.Set(statsBatchesQueried, b.batchesQueried)
	b.statMap.Set(statsPointsQueried, b.pointsQueried)

	if b.et.tm.InfluxDBService == nil {
		return errors.New("InfluxDB not configured, cannot query InfluxDB for batch query")
	}

	con, err := b.et.tm.InfluxDBService.NewNamedClient(b.b.Cluster)
	if err != nil {
		return errors.Wrap(err, "failed to get InfluxDB client")
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
			b.query.SetStartTime(stop.Add(-1 * b.b.Period))
			b.query.SetStopTime(stop)

			qStr := b.query.String()
			b.logger.Println("D! starting next batch query:", qStr)

			// Execute query
			q := influxdb.Query{
				Command: qStr,
			}
			resp, err := con.Query(q)
			if err != nil {
				b.queryErrors.Add(1)
				b.logger.Println("E!", err)
				b.timer.Stop()
				break
			}

			// Collect batches
			for _, res := range resp.Results {
				batches, err := models.ResultToBatches(res, b.byName)
				if err != nil {
					b.logger.Println("E! failed to understand query result:", err)
					b.queryErrors.Add(1)
					continue
				}
				for _, bch := range batches {
					// Set stop time based off query bounds
					if bch.TMax.IsZero() || !b.query.IsGroupedByTime() {
						bch.TMax = stop
					}
					b.batchesQueried.Add(1)
					b.pointsQueried.Add(int64(len(bch.Points)))
					b.timer.Pause()
					err := b.ins[0].CollectBatch(bch)
					if err != nil {
						return err
					}
					b.timer.Resume()
				}
			}
			b.timer.Stop()
		}
	}
}

func (b *QueryNode) runBatch([]byte) error {
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

func (b *QueryNode) stopBatch() {
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
	align     bool
	alignChan chan time.Time
	stopping  chan struct{}
	ticker    *time.Ticker
	mu        sync.Mutex
	wg        sync.WaitGroup
}

func newTimeTicker(every time.Duration, align bool) *timeTicker {
	t := &timeTicker{
		align: align,
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
					now = now.Round(t.every)
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
	next := now.Add(t.every)
	if t.align {
		next = next.Round(t.every)
	}
	return next
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
