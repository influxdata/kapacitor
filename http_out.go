package kapacitor

import (
	"encoding/json"
	"net/http"
	"path"
	"sync"

	"github.com/influxdb/influxdb/influxql"
	imodels "github.com/influxdb/influxdb/models"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/services/httpd"
)

type HTTPOutNode struct {
	node
	c              *pipeline.HTTPOutNode
	result         influxql.Result
	groupSeriesIdx map[models.GroupID]int
	endpoint       string
	routes         []httpd.Route
	mu             sync.Mutex
}

// Create a new  HTTPOutNode which caches the most recent item and exposes it over the HTTP API.
func newHTTPOutNode(et *ExecutingTask, n *pipeline.HTTPOutNode) (*HTTPOutNode, error) {
	hn := &HTTPOutNode{
		node:           node{Node: n, et: et},
		c:              n,
		groupSeriesIdx: make(map[models.GroupID]int),
	}
	et.registerOutput(hn.c.Endpoint, hn)
	hn.node.runF = hn.runOut
	hn.node.stopF = hn.stopOut
	return hn, nil
}

func (h *HTTPOutNode) Endpoint() string {
	return "http://" + h.endpoint
}

func (h *HTTPOutNode) runOut() error {

	hndl := func(w http.ResponseWriter, req *http.Request) {
		h.mu.Lock()
		defer h.mu.Unlock()

		if b, err := json.Marshal(h.result); err != nil {
			httpd.HttpError(
				w,
				err.Error(),
				true,
				http.StatusInternalServerError,
			)
		} else {
			w.Write(b)
		}
	}

	p := path.Join("/", h.et.Task.Name, h.c.Endpoint)

	r := []httpd.Route{{
		Name:        h.Name(),
		Method:      "GET",
		Pattern:     p,
		Gzipped:     true,
		Log:         true,
		HandlerFunc: hndl,
	}}

	h.endpoint = h.et.tm.HTTPDService.Addr().String() + p
	h.routes = r

	err := h.et.tm.HTTPDService.AddRoutes(r)
	if err != nil {
		return err
	}

	switch h.Wants() {
	case pipeline.StreamEdge:
		for p, ok := h.ins[0].NextPoint(); ok; p, ok = h.ins[0].NextPoint() {
			h.updateResultWithPoint(p)
		}
	case pipeline.BatchEdge:
		for b, ok := h.ins[0].NextBatch(); ok; b, ok = h.ins[0].NextBatch() {
			h.updateResultWithBatch(b)
		}
	}

	return nil
}

// Update the result structure with a single point.
func (h *HTTPOutNode) updateResultWithPoint(p models.Point) {
	h.mu.Lock()
	defer h.mu.Unlock()
	batch := &imodels.Row{
		Name:    p.Name,
		Tags:    p.Tags,
		Columns: []string{"time"},
		Values:  make([][]interface{}, 1),
	}

	for f := range p.Fields {
		batch.Columns = append(batch.Columns, f)
	}
	batch.Values[0] = make([]interface{}, len(p.Fields)+1)
	batch.Values[0][0] = p.Time
	for i, c := range batch.Columns[1:] {
		batch.Values[0][i+1] = p.Fields[c]
	}
	if idx, ok := h.groupSeriesIdx[p.Group]; !ok {
		idx = len(h.result.Series)
		h.groupSeriesIdx[p.Group] = idx
		h.result.Series = append(h.result.Series, batch)
	} else {
		h.result.Series[idx] = batch
	}
}

// Update the result structure with a batch.
func (h *HTTPOutNode) updateResultWithBatch(b models.Batch) {
	h.mu.Lock()
	defer h.mu.Unlock()
	batch := &imodels.Row{
		Name:    b.Name,
		Tags:    b.Tags,
		Columns: []string{"time"},
	}
	idx, ok := h.groupSeriesIdx[b.Group]
	if !ok {
		idx = len(h.result.Series)
		h.groupSeriesIdx[b.Group] = idx
		h.result.Series = append(h.result.Series, batch)
	} else {
		h.result.Series[idx] = batch
	}
	if len(b.Points) == 0 {
		return
	}
	p := b.Points[0]
	for f := range p.Fields {
		batch.Columns = append(batch.Columns, f)
	}
	batch.Values = make([][]interface{}, len(b.Points))
	for i, p := range b.Points {
		batch.Values[i] = make([]interface{}, len(p.Fields)+1)
		batch.Values[i][0] = p.Time
		for j, c := range batch.Columns[1:] {
			batch.Values[i][j+1] = p.Fields[c]
		}
	}
}
func (h *HTTPOutNode) stopOut() {
	h.et.tm.HTTPDService.DelRoutes(h.routes)
}
