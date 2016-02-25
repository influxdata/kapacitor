package kapacitor

import (
	"log"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/timer"
)

type UnionNode struct {
	node
	u *pipeline.UnionNode

	// Buffer of points/batches from each parent
	sources [][]models.PointInterface

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
	if u.rename == "" {
		//the calling node is always the last node
		u.rename = u.parents[len(u.parents)-1].Name()
	}
	// Spawn goroutine for each parent
	errors := make(chan error, len(u.ins))
	for i, in := range u.ins {
		t := u.et.tm.TimingService.NewTimer(u.statMap.Get(statAverageExecTime).(timer.Setter))
		go func(index int, e *Edge, t timer.Timer) {
			for p, ok := e.Next(); ok; p, ok = e.Next() {
				union <- srcPoint{
					src: index,
					p:   p,
				}
			}
			errors <- nil
		}(i, in, t)
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

	for {
		select {
		case err := <-errC:
			// One of the parents errored out, return the error.
			return err
		case source, ok := <-union:
			u.timer.Start()
			if !ok {
				// We are done, emit all buffered points
				for {
					more, err := u.emitNext(true)
					if err != nil {
						return err
					}
					if !more {
						return nil
					}
				}
			}
			// Add newest point to buffer
			u.sources[source.src] = append(u.sources[source.src], source.p)

			// Emit the next values
			_, err := u.emitNext(false)
			if err != nil {
				return err
			}
			u.timer.Stop()
		}
	}
}

func (u *UnionNode) emitNext(drain bool) (more bool, err error) {
	more = true

	// Find low water mark
	var mark time.Time
	markSet := false
	for _, values := range u.sources {
		if len(values) > 0 {
			if !markSet || values[0].PointTime().Before(mark) {
				mark = values[0].PointTime()
				markSet = true
			}
		} else if !drain {
			// We can't continue processing until we have
			// at least one value buffered from each parent.
			// Unless we are draining the buffer than we can continue.
			return
		}
	}

	// Emit all values that are at or below the mark.
	remaining := 0
	for i, values := range u.sources {
		var j int
		l := len(values)
		for j = 0; j < l; j++ {
			if !values[j].PointTime().After(mark) {
				err = u.emit(values[j])
				if err != nil {
					return
				}
			} else {
				break
			}
		}
		// Drop values that were emitted
		u.sources[i] = values[j:]
		remaining += len(u.sources[i])
	}
	more = remaining > 0
	return
}

func (u *UnionNode) emit(v models.PointInterface) error {
	u.timer.Pause()
	defer u.timer.Resume()
	switch u.Provides() {
	case pipeline.StreamEdge:
		p := v.(models.Point)
		p.Name = u.rename
		for _, child := range u.outs {
			err := child.CollectPoint(p)
			if err != nil {
				return err
			}
		}
	case pipeline.BatchEdge:
		b := v.(models.Batch)
		b.Name = u.rename
		for _, child := range u.outs {
			err := child.CollectBatch(b)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
