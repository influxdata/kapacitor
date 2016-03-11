package kapacitor

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"time"

	dbmodels "github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor/clock"
	"github.com/influxdata/kapacitor/models"
)

// Replay engine that can replay static data sets against a specific executor and its tasks.
type Replay struct {
	clck   clock.Clock
	Setter clock.Setter
}

// Create a new replay engine.
func NewReplay(c clock.Clock) *Replay {
	return &Replay{
		clck:   c,
		Setter: c,
	}
}

// Replay a data set against an executor.
func (r *Replay) ReplayStream(data io.ReadCloser, stream StreamCollector, recTime bool, precision string) <-chan error {
	src := newReplayStreamSource(data, r.clck)
	go src.replayStream(stream, recTime, precision)
	return src.Err()
}

type replayStreamSource struct {
	data io.Closer
	in   *bufio.Scanner
	clck clock.Clock
	err  chan error
}

func newReplayStreamSource(data io.ReadCloser, clck clock.Clock) *replayStreamSource {
	return &replayStreamSource{
		data: data,
		in:   bufio.NewScanner(data),
		clck: clck,
		err:  make(chan error, 1),
	}
}

func (r *replayStreamSource) Err() <-chan error {
	return r.err
}

func (r *replayStreamSource) replayStream(stream StreamCollector, recTime bool, precision string) {
	defer stream.Close()
	defer r.data.Close()
	start := time.Time{}
	var diff time.Duration
	zero := r.clck.Zero()
	for r.in.Scan() {
		db := r.in.Text()
		if !r.in.Scan() {
			r.err <- fmt.Errorf("invalid replay file format, expected another line")
			return
		}
		rp := r.in.Text()
		if !r.in.Scan() {
			r.err <- fmt.Errorf("invalid replay file format, expected another line")
			return
		}
		points, err := dbmodels.ParsePointsWithPrecision(
			r.in.Bytes(),
			zero,
			precision,
		)
		if err != nil {
			r.err <- err
			return
		}
		if start.IsZero() {
			start = points[0].Time()
			diff = zero.Sub(start)
		}
		var t time.Time
		waitTime := points[0].Time().Add(diff).UTC()
		if !recTime {
			t = waitTime
		} else {
			t = points[0].Time().UTC()
		}
		mp := points[0]
		p := models.Point{
			Database:        db,
			RetentionPolicy: rp,
			Name:            mp.Name(),
			Group:           models.NilGroup,
			Tags:            models.Tags(mp.Tags()),
			Fields:          models.Fields(mp.Fields()),
			Time:            t,
		}
		r.clck.Until(waitTime)
		err = stream.CollectPoint(p)
		if err != nil {
			r.err <- err
			return
		}
	}
	r.err <- r.in.Err()
}

// Replay a data set against an executor.
// If source time is true then the replay will use the times stored in the
// recording instead of the clock time.
func (r *Replay) ReplayBatch(data []io.ReadCloser, batches []BatchCollector, recTime bool) <-chan error {
	src := newReplayBatchSource(data, r.clck)
	src.replayBatch(batches, recTime)
	return src.Err()
}

type replayBatchSource struct {
	data    []io.ReadCloser
	clck    clock.Clock
	allErrs chan error
	err     chan error
}

func newReplayBatchSource(data []io.ReadCloser, clck clock.Clock) *replayBatchSource {
	return &replayBatchSource{
		data:    data,
		clck:    clck,
		allErrs: make(chan error, len(data)),
		err:     make(chan error, 1),
	}
}
func (r *replayBatchSource) Err() <-chan error {
	return r.err
}

func (r *replayBatchSource) replayBatch(batches []BatchCollector, recTime bool) {
	if e, g := len(r.data), len(batches); e != g {
		r.err <- fmt.Errorf("unexpected number of batch collectors. exp %d got %d", e, g)
		return
	}
	for i := range r.data {
		go r.replayBatchFromData(r.data[i], batches[i], recTime)
	}
	go func() {
		// Wait for each one to finish and report first error if any
		for range r.data {
			err := <-r.allErrs
			if err != nil {

				r.err <- err
				return
			}
		}
		r.err <- nil
	}()
}

// Replay the batch data from a single source
func (r *replayBatchSource) replayBatchFromData(data io.ReadCloser, batch BatchCollector, recTime bool) {
	defer batch.Close()
	defer data.Close()

	in := bufio.NewScanner(data)

	// Find relative times
	start := time.Time{}
	var diff time.Duration
	zero := r.clck.Zero()

	for in.Scan() {
		var b models.Batch
		err := json.Unmarshal(in.Bytes(), &b)
		if err != nil {
			r.allErrs <- err
			return
		}
		if len(b.Points) == 0 {
			// do nothing
			continue
		}
		b.Group = models.TagsToGroupID(models.SortedKeys(b.Tags), b.Tags)

		if start.IsZero() {
			start = b.Points[0].Time
			diff = zero.Sub(start)
		}
		// Add tags to all points
		if len(b.Tags) > 0 {
			for i := range b.Points {
				if len(b.Points[i].Tags) == 0 {
					b.Points[i].Tags = b.Tags
				}
			}
		}
		var lastTime time.Time
		if !recTime {
			for i := range b.Points {
				b.Points[i].Time = b.Points[i].Time.Add(diff).UTC()
			}
			lastTime = b.Points[len(b.Points)-1].Time
		} else {
			lastTime = b.Points[len(b.Points)-1].Time.Add(diff).UTC()
		}
		r.clck.Until(lastTime)
		b.TMax = b.Points[len(b.Points)-1].Time
		batch.CollectBatch(b)
	}
	r.allErrs <- in.Err()
}

func WritePointForRecording(w io.Writer, p models.Point, precision string) error {
	fmt.Fprintf(w, "%s\n%s\n", p.Database, p.RetentionPolicy)
	w.Write(p.Bytes(precision))
	w.Write([]byte("\n"))
	return nil
}

func WriteBatchForRecording(w io.Writer, b models.Batch) error {
	enc := json.NewEncoder(w)
	err := enc.Encode(b)
	if err != nil {
		return err
	}
	return nil
}
