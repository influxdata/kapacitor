package main

import (
	"log"
	"os"
	"sort"

	"github.com/influxdata/kapacitor/udf/agent"
)

// Find outliers via the Tukey method. As defined in the README.md
type outlierHandler struct {
	field string
	scale float64
	state *outlierState
	begin *agent.BeginBatch

	agent *agent.Agent
}

func newOutlierHandler(agent *agent.Agent) *outlierHandler {
	return &outlierHandler{agent: agent, state: &outlierState{}, scale: 1.5}
}

// Return the InfoResponse. Describing the properties of this UDF agent.
func (*outlierHandler) Info() (*agent.InfoResponse, error) {
	info := &agent.InfoResponse{
		Wants:    agent.EdgeType_BATCH,
		Provides: agent.EdgeType_BATCH,
		Options: map[string]*agent.OptionInfo{
			"field": {ValueTypes: []agent.ValueType{agent.ValueType_STRING}},
			"scale": {ValueTypes: []agent.ValueType{agent.ValueType_DOUBLE}},
		},
	}
	return info, nil
}

// Initialze the handler based of the provided options.
func (o *outlierHandler) Init(r *agent.InitRequest) (*agent.InitResponse, error) {
	init := &agent.InitResponse{
		Success: true,
		Error:   "",
	}
	for _, opt := range r.Options {
		switch opt.Name {
		case "field":
			o.field = opt.Values[0].Value.(*agent.OptionValue_StringValue).StringValue
		case "scale":
			o.scale = opt.Values[0].Value.(*agent.OptionValue_DoubleValue).DoubleValue
		}
	}

	if o.field == "" {
		init.Success = false
		init.Error = "must supply field"
	}
	if o.scale < 1 {
		init.Success = false
		init.Error += " invalid scale, must be >= 1.0"
	}

	return init, nil
}

// Create a snapshot of the running state of the process.
func (o *outlierHandler) Snapshot() (*agent.SnapshotResponse, error) {
	return &agent.SnapshotResponse{}, nil
}

// Restore a previous snapshot.
func (o *outlierHandler) Restore(req *agent.RestoreRequest) (*agent.RestoreResponse, error) {
	return &agent.RestoreResponse{
		Success: true,
	}, nil
}

// Start working with the next batch
func (o *outlierHandler) BeginBatch(begin *agent.BeginBatch) error {
	o.state.reset()

	// Keep begin batch for later
	o.begin = begin

	return nil
}

func (o *outlierHandler) Point(p *agent.Point) error {
	value := p.FieldsDouble[o.field]
	o.state.update(value, p)
	return nil
}

func (o *outlierHandler) EndBatch(end *agent.EndBatch) error {
	// Get outliers
	outliers := o.state.outliers(o.scale)

	// Send BeginBatch response to Kapacitor
	// with count of outliers
	o.begin.Size = int64(len(outliers))
	o.agent.Responses <- &agent.Response{
		Message: &agent.Response_Begin{
			Begin: o.begin,
		},
	}

	// Send outliers as part of batch
	for _, outlier := range outliers {
		o.agent.Responses <- &agent.Response{
			Message: &agent.Response_Point{
				Point: outlier,
			},
		}
	}

	// End batch
	o.agent.Responses <- &agent.Response{
		Message: &agent.Response_End{
			End: end,
		},
	}
	return nil
}

// Stop the handler gracefully.
func (o *outlierHandler) Stop() {
	close(o.agent.Responses)
}

type outlierState struct {
	entries entries
}

type entry struct {
	value float64
	point *agent.Point
}

type entries []entry

func (e entries) Len() int           { return len(e) }
func (e entries) Less(i, j int) bool { return e[i].value < e[j].value }
func (e entries) Swap(i, j int)      { e[j], e[i] = e[i], e[j] }

func (s *outlierState) reset() {
	s.entries = nil
}

func (s *outlierState) update(value float64, point *agent.Point) {
	s.entries = append(s.entries, entry{value: value, point: point})
}

func (s *outlierState) outliers(scale float64) []*agent.Point {
	first, third, lower, upper := s.bounds(scale)

	max := first + len(s.entries) - third
	outliers := make([]*agent.Point, 0, max)

	// Append lower outliers
	for i := 0; i < first; i++ {
		if s.entries[i].value < lower {
			outliers = append(outliers, s.entries[i].point)
		} else {
			break
		}
	}

	// Append upper outliers
	for i := third + 1; i < len(s.entries); i++ {
		if s.entries[i].value > upper {
			outliers = append(outliers, s.entries[i].point)
		}
	}
	return outliers
}

func (s *outlierState) bounds(scale float64) (first, third int, lower, upper float64) {
	sort.Sort(s.entries)
	ml, mr, _ := s.median(s.entries)
	_, first, fq := s.median(s.entries[:mr])
	third, _, tq := s.median(s.entries[ml+1:])
	iqr := tq - fq
	lower = fq - iqr*scale
	upper = tq + iqr*scale
	return
}

func (s *outlierState) median(data entries) (left, right int, median float64) {
	l := len(data)
	m := l / 2
	if l%2 == 0 {
		left = m
		right = m + 1
		median = (data[left].value + data[right].value) / 2.0
	} else {
		left = m
		right = m
		median = data[m].value
	}
	return
}

func main() {
	a := agent.New(os.Stdin, os.Stdout)
	h := newOutlierHandler(a)
	a.Handler = h

	log.Println("Starting agent")
	a.Start()
	err := a.Wait()
	if err != nil {
		log.Fatal(err)
	}
}
