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

// Replay stream data from a channel source.
func ReplayStreamFromChan(clck clock.Clock, points <-chan models.Point, collector StreamCollector, recTime bool) <-chan error {
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
	points := make(chan models.Point)
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

func replayStreamFromChan(clck clock.Clock, points <-chan models.Point, collector StreamCollector, recTime bool) error {
	defer collector.Close()
	start := time.Time{}
	var diff time.Duration
	zero := clck.Zero()
	for p := range points {
		if start.IsZero() {
			start = p.Time
			diff = zero.Sub(start)
		}
		waitTime := p.Time.Add(diff).UTC()
		if !recTime {
			p.Time = waitTime
		}
		clck.Until(waitTime)
		err := collector.CollectPoint(p)
		if err != nil {
			return err
		}
	}
	return nil
}

func readPointsFromIO(data io.ReadCloser, points chan<- models.Point, precision string) error {
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
		p := models.Point{
			Database:        db,
			RetentionPolicy: rp,
			Name:            mp.Name(),
			Group:           models.NilGroup,
			Tags:            models.Tags(mp.Tags().Map()),
			Fields:          models.Fields(mp.Fields()),
			Time:            mp.Time().UTC(),
		}
		points <- p
	}
	return nil
}

// Replay batch data from a channel source.
func ReplayBatchFromChan(clck clock.Clock, batches []<-chan models.Batch, collectors []BatchCollector, recTime bool) <-chan error {
	errC := make(chan error, 1)
	if e, g := len(batches), len(collectors); e != g {
		errC <- fmt.Errorf("unexpected number of batch collectors. exp %d got %d", e, g)
		return errC
	}

	allErrs := make(chan error, len(batches))
	for i := range batches {
		go func(collector BatchCollector, batches <-chan models.Batch, clck clock.Clock, recTime bool) {
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
		batches := make(chan models.Batch)
		go func(collector BatchCollector, batches <-chan models.Batch, clck clock.Clock, recTime bool) {
			allErrs <- replayBatchFromChan(clck, batches, collector, recTime)
		}(collectors[i], batches, clck, recTime)
		go func(data io.ReadCloser, batches chan<- models.Batch) {
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
func replayBatchFromChan(clck clock.Clock, batches <-chan models.Batch, collector BatchCollector, recTime bool) error {
	defer collector.Close()

	// Find relative times
	var start, tmax time.Time
	var diff time.Duration
	zero := clck.Zero()

	for b := range batches {
		if len(b.Points) == 0 {
			// Emit empty batch
			if b.TMax.IsZero() {
				// Set tmax to last batch if not set.
				b.TMax = tmax
			} else {
				b.TMax = b.TMax.UTC()
				tmax = b.TMax
			}
			if err := collector.CollectBatch(b); err != nil {
				return err
			}
			continue
		}
		if start.IsZero() {
			start = b.Points[0].Time
			diff = zero.Sub(start)
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
		clck.Until(lastTime)
		if lpt := b.Points[len(b.Points)-1].Time; b.TMax.Before(lpt) {
			b.TMax = lpt
		}
		b.TMax = b.TMax.UTC()
		tmax = b.TMax
		if err := collector.CollectBatch(b); err != nil {
			return err
		}
	}
	return nil
}

// Replay the batch data from a single source
func readBatchFromIO(data io.ReadCloser, batches chan<- models.Batch) error {
	defer close(batches)
	defer data.Close()
	dec := json.NewDecoder(data)
	for dec.More() {
		var b models.Batch
		err := dec.Decode(&b)
		if err != nil {
			return err
		}
		if len(b.Points) == 0 {
			// do nothing
			continue
		}
		if b.Group == "" {
			b.Group = models.ToGroupID(
				b.Name,
				b.Tags,
				models.Dimensions{
					ByName:   b.ByName,
					TagNames: models.SortedKeys(b.Tags),
				},
			)
		}
		// Add tags to all points
		if len(b.Tags) > 0 {
			for i := range b.Points {
				if len(b.Points[i].Tags) == 0 {
					b.Points[i].Tags = b.Tags
				}
			}
		}
		batches <- b
	}
	return nil
}

func WritePointForRecording(w io.Writer, p models.Point, precision string) error {
	if _, err := fmt.Fprintf(w, "%s\n%s\n", p.Database, p.RetentionPolicy); err != nil {
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

func WriteBatchForRecording(w io.Writer, b models.Batch) error {
	enc := json.NewEncoder(w)
	err := enc.Encode(b)
	if err != nil {
		return err
	}
	return nil
}
