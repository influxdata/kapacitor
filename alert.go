package kapacitor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"

	"github.com/influxdb/kapacitor/expr"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

type AlertHandler func(pts []*models.Point)

type AlertNode struct {
	node
	a         *pipeline.AlertNode
	endpoint  string
	handlers  []AlertHandler
	predicate *expr.Tree
	funcs     expr.Funcs
	fs        []exprFunc
}

type exprFunc interface {
	name() string
	fnc() expr.Func
}

// Create a new  AlertNode which caches the most recent item and exposes it over the HTTP API.
func newAlertNode(et *ExecutingTask, n *pipeline.AlertNode) (an *AlertNode, err error) {
	an = &AlertNode{
		node: node{Node: n, et: et},
		a:    n,
	}
	an.node.runF = an.runAlert
	// Construct alert handlers
	an.handlers = make([]AlertHandler, 0)
	if n.Post != "" {
		an.handlers = append(an.handlers, an.handlePost)
	}
	// Parse predicate
	an.predicate, err = expr.Parse(n.Predicate)
	if err != nil {
		return nil, err
	}
	if an.predicate.RType() != expr.ReturnBool {
		return nil, fmt.Errorf("Predicate does not evaluate to boolean value %q", n.Predicate)
	}

	// Initialize functions for the predicate
	an.fs = append(an.fs, &sigma{})

	an.funcs = make(expr.Funcs)
	for _, f := range an.fs {
		an.funcs[f.name()] = f.fnc()
	}

	return
}

func (a *AlertNode) runAlert() error {
	switch a.Wants() {
	case pipeline.StreamEdge:
		for p := a.ins[0].NextPoint(); p != nil; p = a.ins[0].NextPoint() {
			if c, err := a.check(p); err != nil {
				return err
			} else if c {
				for _, h := range a.handlers {
					go h([]*models.Point{p})
				}
			}
		}
	case pipeline.BatchEdge:
		for w := a.ins[0].NextBatch(); w != nil; w = a.ins[0].NextBatch() {
			for _, p := range w {
				if c, err := a.check(p); err != nil {
					return err
				} else if c {
					for _, h := range a.handlers {
						go h(w)
					}
					break
				}
			}
		}
	}
	return nil
}

func (a *AlertNode) check(p *models.Point) (bool, error) {
	vars := make(expr.Vars)
	for k, v := range p.Fields {
		if f, ok := v.(float64); ok {
			vars[k] = f
		} else {
			return false, fmt.Errorf("field values must be float64")
		}
	}
	b, err := a.predicate.EvalBool(vars, a.funcs)
	return b, err
}

func (a *AlertNode) handlePost(pts []*models.Point) {
	b, err := json.Marshal(pts)
	if err != nil {
		a.logger.Println("E! failed to marshal points json")
		return
	}
	buf := bytes.NewBuffer(b)
	http.Post(a.a.Post, "application/json", buf)
}

type sigma struct {
	mean     float64
	variance float64
	m2       float64
	n        float64
}

func (s *sigma) name() string {
	return "sigma"
}

func (s *sigma) fnc() expr.Func {
	return s.call
}

func (s *sigma) call(args ...float64) (float64, error) {
	if len(args) != 1 {
		return 0, errors.New("sigma expected exactly one argument")
	}
	x := args[0]
	s.n++
	delta := x - s.mean
	s.mean = s.mean + delta/s.n
	s.m2 = s.m2 + delta*(x-s.mean)
	s.variance = s.m2 / (s.n - 1)

	if s.n < 2 {
		return 0, nil
	}
	return math.Abs(x-s.mean) / math.Sqrt(s.variance), nil

}
