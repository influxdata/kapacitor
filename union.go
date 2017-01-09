package kapacitor

import (
	"log"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

type UnionNode struct {
	node
	u *pipeline.UnionNode

	// Buffer of points/batches from each parent
	sources [][]models.PointInterface
	// the low water marks for each source.
	lowMarks []time.Time

	rename string
}

// Create a new  UnionNode which combines all parent data streams into a single stream.
// No transformation of any kind is performed.
func newUnionNode(et *ExecutingTask, n *pipeline.UnionNode, l *log.Logger) (*UnionNode, error) {
	un := &UnionNode{
		u:    n,
		node: node{Node: n, et: et, logger: l},
	}
	un.node.runF = un.runUnion
	return un, nil
}

func (u *UnionNode) runUnion([]byte) error {
	union := make(chan srcPoint)
	u.rename = u.u.Rename
	// Spawn goroutine for each parent
	errors := make(chan error, len(u.ins))
	for i, in := range u.ins {
		go func(index int, e *Edge) {
			for p, ok := e.Next(); ok; p, ok = e.Next() {
				union <- srcPoint{
					src: index,
					p:   p,
				}
			}
			errors <- nil
		}(i, in)
	}

	// Channel for returning the first if any errors, from parent goroutines.
	errC := make(chan error, 1)

	go func() {
		for range u.ins {
			err := <-errors
			if err != nil {
				errC <- err
				return
			}
		}
		// Close the union channel once all parents are done writing
		close(union)
	}()

	//
	// Emit values received from parents ordered by time.
	//

	// Keep buffer of values from parents so they can be ordered.
	u.sources = make([][]models.PointInterface, len(u.ins))
	u.lowMarks = make([]time.Time, len(u.ins))

	for {
		select {
		case err := <-errC:
			// One of the parents errored out, return the error.
			return err
		case source, ok := <-union:
			u.timer.Start()
			if !ok {
				// We are done, emit all buffered
				return u.emitReady(true)
			}
			// Add newest point to buffer
			u.sources[source.src] = append(u.sources[source.src], source.p)

			// Emit the next values
			err := u.emitReady(false)
			if err != nil {
				return err
			}
			u.timer.Stop()
		}
	}
}

func (u *UnionNode) emitReady(drain bool) error {
	emitted := true
	// Emit all points until nothing changes
	for emitted {
		emitted = false
		// Find low water mark
		var mark time.Time
		validSources := 0
		for i, values := range u.sources {
			sourceMark := u.lowMarks[i]
			if len(values) > 0 {
				t := values[0].PointTime()
				if mark.IsZero() || t.Before(mark) {
					mark = t
				}
				sourceMark = t
			}
			u.lowMarks[i] = sourceMark
			if !sourceMark.IsZero() {
				validSources++
				// Only consider the sourceMark if we are not draining
				if !drain && (mark.IsZero() || sourceMark.Before(mark)) {
					mark = sourceMark
				}
			}
		}
		if !drain && validSources != len(u.sources) {
			// We can't continue processing until we have
			// at least one value from each parent.
			// Unless we are draining the buffer than we can continue.
			return nil
		}

		// Emit all values that are at or below the mark.
		for i, values := range u.sources {
			var j int
			l := len(values)
			for j = 0; j < l; j++ {
				if !values[j].PointTime().After(mark) {
					err := u.emit(values[j])
					if err != nil {
						return err
					}
					// Note that we emitted something
					emitted = true
				} else {
					break
				}
			}
			// Drop values that were emitted
			u.sources[i] = values[j:]
		}
	}
	return nil
}

func (u *UnionNode) emit(v models.PointInterface) error {
	u.timer.Pause()
	defer u.timer.Resume()
	switch u.Provides() {
	case pipeline.StreamEdge:
		p := v.(models.Point)
		if u.rename != "" {
			p.Name = u.rename
		}
		for _, child := range u.outs {
			err := child.CollectPoint(p)
			if err != nil {
				return err
			}
		}
	case pipeline.BatchEdge:
		b := v.(models.Batch)
		if u.rename != "" {
			b.Name = u.rename
		}
		for _, child := range u.outs {
			err := child.CollectBatch(b)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
