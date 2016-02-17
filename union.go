package kapacitor

import (
	"log"
	"time"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/timer"
)

type UnionNode struct {
	node
	u      *pipeline.UnionNode
	timers []timer.Timer
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
	rename := u.u.Rename
	if rename == "" {
		//the calling node is always the last node
		rename = u.parents[len(u.parents)-1].Name()
	}
	errors := make(chan error, len(u.ins))
	for _, in := range u.ins {
		t := u.et.tm.TimingService.NewTimer()
		u.timers = append(u.timers, t)
		go func(e *Edge, t timer.Timer) {
			switch u.Wants() {
			case pipeline.StreamEdge:
				for p, ok := e.NextPoint(); ok; p, ok = e.NextPoint() {
					t.Start()
					p.Name = rename
					t.Stop()
					for _, out := range u.outs {
						err := out.CollectPoint(p)
						if err != nil {
							errors <- err
							return
						}
					}
				}
			case pipeline.BatchEdge:
				for b, ok := e.NextBatch(); ok; b, ok = e.NextBatch() {
					t.Start()
					b.Name = rename
					t.Stop()
					for _, out := range u.outs {
						err := out.CollectBatch(b)
						if err != nil {
							errors <- err
							return
						}
					}
				}
			}
			errors <- nil
		}(in, t)
	}

	for range u.ins {
		err := <-errors
		if err != nil {
			return err
		}
	}
	return nil
}

func (u *UnionNode) nodeExecTime() time.Duration {
	sum := 0.0
	total := 0.0
	for _, t := range u.timers {
		avg, count := t.AverageTime()
		sum += float64(avg) * float64(count)
		total += float64(count)
	}
	if total == 0 {
		return 0
	}
	return time.Duration(sum / total)
}
