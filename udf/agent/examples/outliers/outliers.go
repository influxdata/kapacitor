package main

import (
	"log"
	"os"
	"sort"

	"github.com/influxdata/kapacitor/udf"
	"github.com/influxdata/kapacitor/udf/agent"
)

// Find outliers via the Tukey method. As defined in the README.md
type outlierHandler struct {
	field string
	scale float64
	state *outlierState
	begin *udf.BeginBatch

	agent *agent.Agent
}

func newOutlierHandler(agent *agent.Agent) *outlierHandler {
	return &outlierHandler{agent: agent, state: &outlierState{}, scale: 1.5}
}

// Return the InfoResponse. Describing the properties of this UDF agent.
func (*outlierHandler) Info() (*udf.InfoResponse, error) {
	info := &udf.InfoResponse{
		Wants:    udf.EdgeType_BATCH,
		Provides: udf.EdgeType_BATCH,
		Options: map[string]*udf.OptionInfo{
			"field": {ValueTypes: []udf.ValueType{udf.ValueType_STRING}},
			"scale": {ValueTypes: []udf.ValueType{udf.ValueType_DOUBLE}},
		},
	}
	return info, nil
}

// Initialze the handler based of the provided options.
func (o *outlierHandler) Init(r *udf.InitRequest) (*udf.InitResponse, error) {
	init := &udf.InitResponse{
		Success: true,
		Error:   "",
	}
	for _, opt := range r.Options {
		switch opt.Name {
		case "field":
			o.field = opt.Values[0].Value.(*udf.OptionValue_StringValue).StringValue
		case "scale":
			o.scale = opt.Values[0].Value.(*udf.OptionValue_DoubleValue).DoubleValue
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
func (o *outlierHandler) Snaphost() (*udf.SnapshotResponse, error) {
	return &udf.SnapshotResponse{}, nil
}

// Restore a previous snapshot.
func (o *outlierHandler) Restore(req *udf.RestoreRequest) (*udf.RestoreResponse, error) {
	return &udf.RestoreResponse{
		Success: true,
	}, nil
}

// Start working with the next batch
func (o *outlierHandler) BeginBatch(begin *udf.BeginBatch) error {
	o.state.reset()

	// Keep begin batch for later
	o.begin = begin

	return nil
}

func (o *outlierHandler) Point(p *udf.Point) error {
	value := p.FieldsDouble[o.field]
	o.state.update(value, p)
	return nil
}

func (o *outlierHandler) EndBatch(end *udf.EndBatch) error {
	// Get outliers
	outliers := o.state.outliers(o.scale)

	// Send BeginBatch response to Kapacitor
	// with count of outliers
	o.begin.Size = int64(len(outliers))
	o.agent.Responses <- &udf.Response{
		Message: &udf.Response_Begin{
			Begin: o.begin,
		},
	}

	// Send outliers as part of batch
	for _, outlier := range outliers {
		o.agent.Responses <- &udf.Response{
			Message: &udf.Response_Point{
				Point: outlier,
			},
		}
	}

	// End batch
	o.agent.Responses <- &udf.Response{
		Message: &udf.Response_End{
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
	point *udf.Point
}

type entries []entry

func (e entries) Len() int           { return len(e) }
func (e entries) Less(i, j int) bool { return e[i].value < e[j].value }
func (e entries) Swap(i, j int)      { e[j], e[i] = e[i], e[j] }

func (s *outlierState) reset() {
	s.entries = nil
}

func (s *outlierState) update(value float64, point *udf.Point) {
	s.entries = append(s.entries, entry{value: value, point: point})
}

func (s *outlierState) outliers(scale float64) []*udf.Point {
	first, third, lower, upper := s.bounds(scale)

	max := first + len(s.entries) - third
	outliers := make([]*udf.Point, 0, max)

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
