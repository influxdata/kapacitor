package kapacitor

import (
	"bytes"
	"log"
	"sync"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/influxdb"
	"github.com/influxdata/kapacitor/models"
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
}

func newInfluxDBOutNode(et *ExecutingTask, n *pipeline.InfluxDBOutNode, l *log.Logger) (*InfluxDBOutNode, error) {
	if et.tm.InfluxDBService == nil {
		return nil, errors.New("no InfluxDB cluster configured cannot use the InfluxDBOutNode")
	}
	cli, err := et.tm.InfluxDBService.NewNamedClient(n.Cluster)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get InfluxDB client")
	}
	in := &InfluxDBOutNode{
		node: node{Node: n, et: et, logger: l},
		i:    n,
		wb:   newWriteBuffer(int(n.Buffer), n.FlushInterval, cli),
	}
	in.node.runF = in.runOut
	in.node.stopF = in.stopOut
	in.wb.i = in
	return in, nil
}

func (i *InfluxDBOutNode) runOut([]byte) error {
	i.pointsWritten = &expvar.Int{}
	i.writeErrors = &expvar.Int{}

	i.statMap.Set(statsInfluxDBPointsWritten, i.pointsWritten)
	i.statMap.Set(statsInfluxDBWriteErrors, i.writeErrors)

	// Start the write buffer
	i.wb.start()

	// Create the database and retention policy
	if i.i.CreateFlag {
		err := func() error {
			cli, err := i.et.tm.InfluxDBService.NewNamedClient(i.i.Cluster)
			if err != nil {
				return err
			}
			var createDb bytes.Buffer
			createDb.WriteString("CREATE DATABASE ")
			createDb.WriteString(influxql.QuoteIdent(i.i.Database))
			if i.i.RetentionPolicy != "" {
				createDb.WriteString(" WITH NAME ")
				createDb.WriteString(influxql.QuoteIdent(i.i.RetentionPolicy))
			}
			_, err = cli.Query(influxdb.Query{Command: createDb.String()})
			if err != nil {
				return err
			}
			return nil
		}()
		if err != nil {
			i.logger.Printf("E! failed to create database %q on cluster %q: %v", i.i.Database, i.i.Cluster, err)
		}
	}

	switch i.Wants() {
	case pipeline.StreamEdge:
		for p, ok := i.ins[0].NextPoint(); ok; p, ok = i.ins[0].NextPoint() {
			i.timer.Start()
			batch := models.Batch{
				Name:   p.Name,
				Group:  p.Group,
				Tags:   p.Tags,
				ByName: p.Dimensions.ByName,
				Points: []models.BatchPoint{models.BatchPointFromPoint(p)},
			}
			err := i.write(p.Database, p.RetentionPolicy, batch)
			if err != nil {
				return err
			}
			i.timer.Stop()
		}
	case pipeline.BatchEdge:
		for b, ok := i.ins[0].NextBatch(); ok; b, ok = i.ins[0].NextBatch() {
			i.timer.Start()
			err := i.write("", "", b)
			if err != nil {
				return err
			}
			i.timer.Stop()
		}
	}
	return nil
}

func (i *InfluxDBOutNode) stopOut() {
	i.wb.flush()
	i.wb.abort()
}

func (i *InfluxDBOutNode) write(db, rp string, batch models.Batch) error {
	if i.i.Database != "" {
		db = i.i.Database
	}
	if i.i.RetentionPolicy != "" {
		rp = i.i.RetentionPolicy
	}
	name := i.i.Measurement
	if name == "" {
		name = batch.Name
	}

	points := make([]influxdb.Point, len(batch.Points))
	for j, p := range batch.Points {
		var tags map[string]string
		if len(i.i.Tags) > 0 {
			tags = make(map[string]string, len(p.Tags)+len(i.i.Tags))
			for k, v := range p.Tags {
				tags[k] = v
			}
			for k, v := range i.i.Tags {
				tags[k] = v
			}
		} else {
			tags = p.Tags
		}
		points[j] = influxdb.Point{
			Name:   name,
			Tags:   tags,
			Fields: p.Fields,
			Time:   p.Time,
		}
	}
	bpc := influxdb.BatchPointsConfig{
		Database:         db,
		RetentionPolicy:  rp,
		WriteConsistency: i.i.WriteConsistency,
		Precision:        i.i.Precision,
	}
	i.wb.enqueue(bpc, points)
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
					w.i.logger.Println("E! failed to write points to InfluxDB:", err)
					break
				}
				w.buffer[qe.bpc] = bp
			}
			bp.AddPoints(qe.points)
			// Check if we hit buffer size
			if len(bp.Points()) >= w.size {
				err = w.write(bp)
				if err != nil {
					w.i.logger.Println("E! failed to write points to InfluxDB:", err)
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
			w.i.logger.Println("E! failed to write points to InfluxDB:", err)
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
