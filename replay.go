package kapacitor

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"time"

	dbmodels "github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor/clock"
	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
)

// Replay stream data from a channel source.
func ReplayStreamFromChan(clck clock.Clock, points <-chan edge.PointMessage, collector StreamCollector, recTime bool) <-chan error {
	errC := make(chan error, 1)
	go func() {
		errC <- replayStreamFromChan(clck, points, collector, recTime)
	}()
	return errC
}

// Replay stream data from an IO source.
func ReplayStreamFromIO(clck clock.Clock, data io.ReadCloser, collector StreamCollector, recTime bool, precision string) <-chan error {
	allErrs := make(chan error, 2)
	errC := make(chan error, 1)
	points := make(chan edge.PointMessage)
	go func() {
		allErrs <- replayStreamFromChan(clck, points, collector, recTime)
	}()
	go func() {
		allErrs <- readPointsFromIO(data, points, precision)
	}()
	go func() {
		for i := 0; i < cap(allErrs); i++ {
			err := <-allErrs
			if err != nil {
				errC <- err
				return
			}
		}
		errC <- nil
	}()
	return errC
}

func replayStreamFromChan(clck clock.Clock, points <-chan edge.PointMessage, collector StreamCollector, recTime bool) error {
	defer collector.Close()
	start := time.Time{}
	var diff time.Duration
	zero := clck.Zero()
	for p := range points {
		if start.IsZero() {
			start = p.Time()
			diff = zero.Sub(start)
		}
		waitTime := p.Time().Add(diff).UTC()
		if !recTime {
			p = p.ShallowCopy()
			p.SetTime(waitTime)
		}
		clck.Until(waitTime)
		err := collector.CollectPoint(p)
		if err != nil {
			return err
		}
	}
	return nil
}

func readPointsFromIO(data io.ReadCloser, points chan<- edge.PointMessage, precision string) error {
	defer data.Close()
	defer close(points)

	now := time.Time{}

	in := bufio.NewScanner(data)
	for in.Scan() {
		db := in.Text()
		if !in.Scan() {
			return fmt.Errorf("invalid replay file format, expected another line")
		}
		rp := in.Text()
		if !in.Scan() {
			return fmt.Errorf("invalid replay file format, expected another line")
		}
		mps, err := dbmodels.ParsePointsWithPrecision(
			in.Bytes(),
			now,
			precision,
		)
		if err != nil {
			return err
		}
		mp := mps[0]
		p := edge.NewPointMessage(
			mp.Name(),
			db,
			rp,
			models.Dimensions{},
			models.Fields(mp.Fields()),
			models.Tags(mp.Tags().Map()),
			mp.Time().UTC(),
		)
		points <- p
	}
	return nil
}

// Replay batch data from a channel source.
func ReplayBatchFromChan(clck clock.Clock, batches []<-chan edge.BufferedBatchMessage, collectors []BatchCollector, recTime bool) <-chan error {
	errC := make(chan error, 1)
	if e, g := len(batches), len(collectors); e != g {
		errC <- fmt.Errorf("unexpected number of batch collectors. exp %d got %d", e, g)
		return errC
	}

	allErrs := make(chan error, len(batches))
	for i := range batches {
		go func(collector BatchCollector, batches <-chan edge.BufferedBatchMessage, clck clock.Clock, recTime bool) {
			allErrs <- replayBatchFromChan(clck, batches, collector, recTime)
		}(collectors[i], batches[i], clck, recTime)
	}
	go func() {
		// Wait for each one to finish and report first error if any
		for i := 0; i < cap(allErrs); i++ {
			err := <-allErrs
			if err != nil {
				errC <- err
				return
			}
		}
		errC <- nil
	}()
	return errC

}

// Replay batch data from an IO source.
func ReplayBatchFromIO(clck clock.Clock, data []io.ReadCloser, collectors []BatchCollector, recTime bool) <-chan error {
	errC := make(chan error, 1)
	if e, g := len(data), len(collectors); e != g {
		errC <- fmt.Errorf("unexpected number of batch collectors. exp %d got %d", e, g)
		return errC
	}

	allErrs := make(chan error, len(data)*2)
	for i := range data {
		batches := make(chan edge.BufferedBatchMessage)
		go func(collector BatchCollector, batches <-chan edge.BufferedBatchMessage, clck clock.Clock, recTime bool) {
			allErrs <- replayBatchFromChan(clck, batches, collector, recTime)
		}(collectors[i], batches, clck, recTime)
		go func(data io.ReadCloser, batches chan<- edge.BufferedBatchMessage) {
			allErrs <- readBatchFromIO(data, batches)
		}(data[i], batches)
	}
	go func() {
		// Wait for each one to finish and report first error if any
		for i := 0; i < cap(allErrs); i++ {
			err := <-allErrs
			if err != nil {
				errC <- err
				return
			}
		}
		errC <- nil
	}()
	return errC
}

// Replay the batch data from a single source
func replayBatchFromChan(clck clock.Clock, batches <-chan edge.BufferedBatchMessage, collector BatchCollector, recTime bool) error {
	defer collector.Close()

	// Find relative times
	var start, tmax time.Time
	var diff time.Duration
	zero := clck.Zero()

	for b := range batches {
		if len(b.Points()) == 0 {
			// Emit empty batch
			if b.Begin().Time().IsZero() {
				// Set tmax to last batch if not set.
				b.Begin().SetTime(tmax)
			} else {
				tmax = b.Begin().Time().UTC()
				b.Begin().SetTime(tmax)
			}
			if err := collector.CollectBatch(b); err != nil {
				return err
			}
			continue
		}
		points := b.Points()
		if start.IsZero() {
			start = points[0].Time()
			diff = zero.Sub(start)
		}
		var lastTime time.Time
		if !recTime {
			for i := range points {
				points[i].SetTime(points[i].Time().Add(diff).UTC())
			}
			lastTime = points[len(points)-1].Time()
		} else {
			lastTime = points[len(points)-1].Time().Add(diff).UTC()
		}
		clck.Until(lastTime)
		if lpt := points[len(points)-1].Time(); b.Begin().Time().Before(lpt) {
			b.Begin().SetTime(lpt)
		}
		tmax = b.Begin().Time().UTC()
		b.Begin().SetTime(tmax)
		if err := collector.CollectBatch(b); err != nil {
			return err
		}
	}
	return nil
}

// Replay the batch data from a single source
func readBatchFromIO(data io.ReadCloser, batches chan<- edge.BufferedBatchMessage) error {
	defer close(batches)
	defer data.Close()
	dec := edge.NewBufferedBatchMessageDecoder(data)
	for dec.More() {
		b, err := dec.Decode()
		if err != nil {
			return err
		}
		if len(b.Points()) == 0 {
			// do nothing
			continue
		}
		batches <- b
	}
	return nil
}

func WritePointForRecording(w io.Writer, p edge.PointMessage, precision string) error {
	if _, err := fmt.Fprintf(w, "%s\n%s\n", p.Database(), p.RetentionPolicy()); err != nil {
		return err
	}
	if _, err := w.Write(p.Bytes(precision)); err != nil {
		return err
	}
	if _, err := w.Write([]byte("\n")); err != nil {
		return err
	}
	return nil
}

func WriteBatchForRecording(w io.Writer, b edge.BufferedBatchMessage) error {
	enc := json.NewEncoder(w)
	err := enc.Encode(b)
	if err != nil {
		return err
	}
	return nil
}
