package kapacitor

import (
	"log"
	"sync"
	"time"

	client "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

const (
	statsInfluxDBPointsWritten = "points_written"
)

type InfluxDBOutNode struct {
	node
	i  *pipeline.InfluxDBOutNode
	wb *writeBuffer
}

func newInfluxDBOutNode(et *ExecutingTask, n *pipeline.InfluxDBOutNode, l *log.Logger) (*InfluxDBOutNode, error) {
	in := &InfluxDBOutNode{
		node: node{Node: n, et: et, logger: l},
		i:    n,
		wb:   newWriteBuffer(int(n.Buffer), n.FlushInterval),
	}
	in.node.runF = in.runOut
	in.node.stopF = in.stopOut
	in.wb.i = in
	return in, nil
}

func (i *InfluxDBOutNode) runOut([]byte) error {
	i.statMap.Add(statsInfluxDBPointsWritten, 0)
	// Start the write buffer
	i.wb.start()

	switch i.Wants() {
	case pipeline.StreamEdge:
		for p, ok := i.ins[0].NextPoint(); ok; p, ok = i.ins[0].NextPoint() {
			i.timer.Start()
			batch := models.Batch{
				Name:   p.Name,
				Group:  p.Group,
				Tags:   p.Tags,
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

	var err error
	points := make([]*client.Point, len(batch.Points))
	for j, p := range batch.Points {
		var tags models.Tags
		if len(i.i.Tags) > 0 {
			tags = make(models.Tags, len(p.Tags)+len(i.i.Tags))
			for k, v := range p.Tags {
				tags[k] = v
			}
			for k, v := range i.i.Tags {
				tags[k] = v
			}
		} else {
			tags = p.Tags
		}
		points[j], err = client.NewPoint(
			name,
			tags,
			p.Fields,
			p.Time,
		)
		if err != nil {
			return err
		}
	}
	bpc := client.BatchPointsConfig{
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
	buffer        map[client.BatchPointsConfig]client.BatchPoints

	flushing chan struct{}
	flushed  chan struct{}

	stopping chan struct{}
	wg       sync.WaitGroup
	conn     client.Client

	i *InfluxDBOutNode
}

type queueEntry struct {
	bpc    client.BatchPointsConfig
	points []*client.Point
}

func newWriteBuffer(size int, flushInterval time.Duration) *writeBuffer {
	return &writeBuffer{
		size:          size,
		flushInterval: flushInterval,
		flushing:      make(chan struct{}),
		flushed:       make(chan struct{}),
		queue:         make(chan queueEntry),
		buffer:        make(map[client.BatchPointsConfig]client.BatchPoints),
		stopping:      make(chan struct{}),
	}
}

func (w *writeBuffer) enqueue(bpc client.BatchPointsConfig, points []*client.Point) {
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
				bp, err = client.NewBatchPoints(qe.bpc)
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

func (w *writeBuffer) write(bp client.BatchPoints) error {
	var err error
	if w.conn == nil {
		w.conn, err = w.i.et.tm.InfluxDBService.NewClient()
		if err != nil {
			return err
		}
	}
	w.i.statMap.Add(statsInfluxDBPointsWritten, int64(len(bp.Points())))
	return w.conn.Write(bp)
}
