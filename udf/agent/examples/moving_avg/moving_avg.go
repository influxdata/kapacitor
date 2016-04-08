package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"log"
	"os"

	"github.com/influxdata/kapacitor/udf"
	"github.com/influxdata/kapacitor/udf/agent"
)

// An Agent.Handler that computes a moving average of the data it receives.
type avgHandler struct {
	field string
	as    string
	size  int
	state map[string]*avgState

	agent *agent.Agent
}

// The state required to compute the moving average.
type avgState struct {
	Size   int
	Window []float64
	Avg    float64
}

// Update the moving average with the next data point.
func (a *avgState) update(value float64) float64 {
	l := len(a.Window)
	if a.Size == l {
		a.Avg += value/float64(l) - a.Window[0]/float64(l)
		a.Window = a.Window[1:]
	} else {
		a.Avg = (value + float64(l)*a.Avg) / float64(l+1)
	}
	a.Window = append(a.Window, value)
	return a.Avg
}

func newMovingAvgHandler(a *agent.Agent) *avgHandler {
	return &avgHandler{
		state: make(map[string]*avgState),
		as:    "avg",
		agent: a,
	}
}

// Return the InfoResponse. Describing the properties of this UDF agent.
func (a *avgHandler) Info() (*udf.InfoResponse, error) {
	info := &udf.InfoResponse{
		Wants:    udf.EdgeType_STREAM,
		Provides: udf.EdgeType_STREAM,
		Options: map[string]*udf.OptionInfo{
			"field": {ValueTypes: []udf.ValueType{udf.ValueType_STRING}},
			"size":  {ValueTypes: []udf.ValueType{udf.ValueType_INT}},
			"as":    {ValueTypes: []udf.ValueType{udf.ValueType_STRING}},
		},
	}
	return info, nil
}

// Initialze the handler based of the provided options.
func (a *avgHandler) Init(r *udf.InitRequest) (*udf.InitResponse, error) {
	init := &udf.InitResponse{
		Success: true,
		Error:   "",
	}
	for _, opt := range r.Options {
		switch opt.Name {
		case "field":
			a.field = opt.Values[0].Value.(*udf.OptionValue_StringValue).StringValue
		case "size":
			a.size = int(opt.Values[0].Value.(*udf.OptionValue_IntValue).IntValue)
		case "as":
			a.as = opt.Values[0].Value.(*udf.OptionValue_StringValue).StringValue
		}
	}

	if a.field == "" {
		init.Success = false
		init.Error += " must supply field"
	}
	if a.size == 0 {
		init.Success = false
		init.Error += " must supply window size"
	}
	if a.as == "" {
		init.Success = false
		init.Error += " invalid as name provided"
	}

	return init, nil
}

// Create a snapshot of the running state of the process.
func (a *avgHandler) Snaphost() (*udf.SnapshotResponse, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(a.state)

	return &udf.SnapshotResponse{
		Snapshot: buf.Bytes(),
	}, nil
}

// Restore a previous snapshot.
func (a *avgHandler) Restore(req *udf.RestoreRequest) (*udf.RestoreResponse, error) {
	buf := bytes.NewReader(req.Snapshot)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&a.state)
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	return &udf.RestoreResponse{
		Success: err == nil,
		Error:   msg,
	}, nil
}

// This handler does not do batching
func (a *avgHandler) BeginBatch(*udf.BeginBatch) error {
	return errors.New("batching not supported")
}

// Receive a point and compute the average.
// Send a response with the average value.
func (a *avgHandler) Point(p *udf.Point) error {
	// Update the moving average.
	value := p.FieldsDouble[a.field]
	state := a.state[p.Group]
	if state == nil {
		state = &avgState{Size: a.size}
		a.state[p.Group] = state
	}
	avg := state.update(value)

	// Re-use the existing point so we keep the same tags etc.
	p.FieldsDouble = map[string]float64{a.as: avg}
	p.FieldsInt = nil
	p.FieldsString = nil
	// Send point with average value.
	a.agent.Responses <- &udf.Response{
		Message: &udf.Response_Point{
			Point: p,
		},
	}
	return nil
}

// This handler does not do batching
func (a *avgHandler) EndBatch(*udf.EndBatch) error {
	return errors.New("batching not supported")
}

// Stop the handler gracefully.
func (a *avgHandler) Stop() {
	close(a.agent.Responses)
}

func main() {
	a := agent.New(os.Stdin, os.Stdout)
	h := newMovingAvgHandler(a)
	a.Handler = h

	log.Println("Starting agent")
	a.Start()
	err := a.Wait()
	if err != nil {
		log.Fatal(err)
	}
}
