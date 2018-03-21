package kapacitor

import (
	"bytes"
	"sync"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/influxdb"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/pkg/errors"
)

const (
	statsInfluxDBPointsWritten = "points_written"
	statsInfluxDBWriteErrors   = "write_errors"
)

type InfluxDBOutNode struct {
	node
	i  *pipeline.InfluxDBOutNode
	wb *writeBuffer

	pointsWritten *expvar.Int
	writeErrors   *expvar.Int

	batchBuffer *edge.BatchBuffer
}

func newInfluxDBOutNode(et *ExecutingTask, n *pipeline.InfluxDBOutNode, d NodeDiagnostic) (*InfluxDBOutNode, error) {
	if et.tm.InfluxDBService == nil {
		return nil, errors.New("no InfluxDB cluster configured cannot use the InfluxDBOutNode")
	}
	cli, err := et.tm.InfluxDBService.NewNamedClient(n.Cluster)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get InfluxDB client")
	}
	in := &InfluxDBOutNode{
		node:        node{Node: n, et: et, diag: d},
		i:           n,
		wb:          newWriteBuffer(int(n.Buffer), n.FlushInterval, cli),
		batchBuffer: new(edge.BatchBuffer),
	}
	in.node.runF = in.runOut
	in.node.stopF = in.stopOut
	in.wb.i = in
	return in, nil
}

func (n *InfluxDBOutNode) runOut([]byte) error {
	n.pointsWritten = &expvar.Int{}
	n.writeErrors = &expvar.Int{}

	n.statMap.Set(statsInfluxDBPointsWritten, n.pointsWritten)
	n.statMap.Set(statsInfluxDBWriteErrors, n.writeErrors)

	// Start the write buffer
	n.wb.start()

	// Create the database and retention policy
	if n.i.CreateFlag {
		err := func() error {
			cli, err := n.et.tm.InfluxDBService.NewNamedClient(n.i.Cluster)
			if err != nil {
				return err
			}
			var createDb bytes.Buffer
			createDb.WriteString("CREATE DATABASE ")
			createDb.WriteString(influxql.QuoteIdent(n.i.Database))
			if n.i.RetentionPolicy != "" {
				createDb.WriteString(" WITH NAME ")
				createDb.WriteString(influxql.QuoteIdent(n.i.RetentionPolicy))
			}
			_, err = cli.Query(influxdb.Query{Command: createDb.String()})
			if err != nil {
				return err
			}
			return nil
		}()
		if err != nil {
			n.diag.Error("failed to create database", err, keyvalue.KV("database", n.i.Database), keyvalue.KV("cluster", n.i.Cluster))
		}
	}

	// Setup consumer
	consumer := edge.NewConsumerWithReceiver(
		n.ins[0],
		edge.NewReceiverFromForwardReceiverWithStats(
			n.outs,
			edge.NewTimedForwardReceiver(n.timer, n),
		),
	)
	return consumer.Consume()
}

func (n *InfluxDBOutNode) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	return nil, n.batchBuffer.BeginBatch(begin)
}

func (n *InfluxDBOutNode) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	return nil, n.batchBuffer.BatchPoint(bp)
}

func (n *InfluxDBOutNode) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return n.BufferedBatch(n.batchBuffer.BufferedBatchMessage(end))
}

func (n *InfluxDBOutNode) BufferedBatch(batch edge.BufferedBatchMessage) (edge.Message, error) {
	n.write("", "", batch)
	return batch, nil
}

func (n *InfluxDBOutNode) Point(p edge.PointMessage) (edge.Message, error) {
	batch := edge.NewBufferedBatchMessage(
		edge.NewBeginBatchMessage(
			p.Name(),
			p.Tags(),
			p.Dimensions().ByName,
			p.Time(),
			1,
		),
		[]edge.BatchPointMessage{
			edge.NewBatchPointMessage(
				p.Fields(),
				p.Tags(),
				p.Time(),
			),
		},
		edge.NewEndBatchMessage(),
	)
	n.write(p.Database(), p.RetentionPolicy(), batch)
	return p, nil
}

func (n *InfluxDBOutNode) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (n *InfluxDBOutNode) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (n *InfluxDBOutNode) Done() {}

func (n *InfluxDBOutNode) stopOut() {
	n.wb.flush()
	n.wb.abort()
}

func (n *InfluxDBOutNode) write(db, rp string, batch edge.BufferedBatchMessage) error {
	if n.i.Database != "" {
		db = n.i.Database
	}
	if n.i.RetentionPolicy != "" {
		rp = n.i.RetentionPolicy
	}
	name := n.i.Measurement
	if name == "" {
		name = batch.Name()
	}

	points := make([]influxdb.Point, len(batch.Points()))
	for j, p := range batch.Points() {
		var tags map[string]string
		if len(n.i.Tags) > 0 {
			tags = make(map[string]string, len(p.Tags())+len(n.i.Tags))
			for k, v := range p.Tags() {
				tags[k] = v
			}
			for k, v := range n.i.Tags {
				tags[k] = v
			}
		} else {
			tags = p.Tags()
		}
		points[j] = influxdb.Point{
			Name:   name,
			Tags:   tags,
			Fields: p.Fields(),
			Time:   p.Time(),
		}
	}
	bpc := influxdb.BatchPointsConfig{
		Database:         db,
		RetentionPolicy:  rp,
		WriteConsistency: n.i.WriteConsistency,
		Precision:        n.i.Precision,
	}
	n.wb.enqueue(bpc, points)
	return nil
}

type writeBuffer struct {
	size          int
	flushInterval time.Duration
	errC          chan error
	queue         chan queueEntry
	buffer        map[influxdb.BatchPointsConfig]influxdb.BatchPoints

	flushing chan struct{}
	flushed  chan struct{}

	stopping chan struct{}
	wg       sync.WaitGroup
	cli      influxdb.Client

	i *InfluxDBOutNode
}

type queueEntry struct {
	bpc    influxdb.BatchPointsConfig
	points []influxdb.Point
}

func newWriteBuffer(size int, flushInterval time.Duration, cli influxdb.Client) *writeBuffer {
	return &writeBuffer{
		cli:           cli,
		size:          size,
		flushInterval: flushInterval,
		flushing:      make(chan struct{}),
		flushed:       make(chan struct{}),
		queue:         make(chan queueEntry),
		buffer:        make(map[influxdb.BatchPointsConfig]influxdb.BatchPoints),
		stopping:      make(chan struct{}),
	}
}

func (w *writeBuffer) enqueue(bpc influxdb.BatchPointsConfig, points []influxdb.Point) {
	qe := queueEntry{
		bpc:    bpc,
		points: points,
	}
	select {
	case w.queue <- qe:
	case <-w.stopping:
	}
}

func (w *writeBuffer) start() {
	w.wg.Add(1)
	go w.run()
}

func (w *writeBuffer) flush() {
	w.flushing <- struct{}{}
	<-w.flushed
}

func (w *writeBuffer) abort() {
	close(w.stopping)
	w.wg.Wait()
}

func (w *writeBuffer) run() {
	defer w.wg.Done()
	flushTick := time.NewTicker(w.flushInterval)
	defer flushTick.Stop()
	var err error
	for {
		select {
		case qe := <-w.queue:
			// Read incoming points off queue
			bp, ok := w.buffer[qe.bpc]
			if !ok {
				bp, err = influxdb.NewBatchPoints(qe.bpc)
				if err != nil {
					w.i.diag.Error("failed to write points to InfluxDB", err)
					break
				}
				w.buffer[qe.bpc] = bp
			}
			bp.AddPoints(qe.points)
			// Check if we hit buffer size
			if len(bp.Points()) >= w.size {
				err = w.write(bp)
				if err != nil {
					w.i.diag.Error("failed to write points to InfluxDB", err)
				}
				delete(w.buffer, qe.bpc)
			}
		case <-w.flushing:
			// Explicit flush called
			w.writeAll()
			w.flushed <- struct{}{}
		case <-flushTick.C:
			// Flush all points after flush interval timeout
			w.writeAll()
		case <-w.stopping:
			return
		}
	}
}

func (w *writeBuffer) writeAll() {
	for bpc, bp := range w.buffer {
		err := w.write(bp)
		if err != nil {
			w.i.diag.Error("failed to write points to InfluxDB", err)
		}
		delete(w.buffer, bpc)
	}
}

func (w *writeBuffer) write(bp influxdb.BatchPoints) error {
	err := w.cli.Write(bp)
	if err != nil {
		w.i.writeErrors.Add(1)
		return err
	}
	w.i.pointsWritten.Add(int64(len(bp.Points())))
	return nil
}
