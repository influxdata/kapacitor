package kapacitor

import (
	"encoding/json"
	"log"
	"net/http"
	"path"
	"sync"

	"github.com/influxdata/influxdb/influxql"
	imodels "github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/services/httpd"
)

type HTTPOutNode struct {
	node
	c              *pipeline.HTTPOutNode
	result         *influxql.Result
	groupSeriesIdx map[models.GroupID]int
	endpoint       string
	routes         []httpd.Route
	mu             sync.RWMutex
}

// Create a new  HTTPOutNode which caches the most recent item and exposes it over the HTTP API.
func newHTTPOutNode(et *ExecutingTask, n *pipeline.HTTPOutNode, l *log.Logger) (*HTTPOutNode, error) {
	hn := &HTTPOutNode{
		node:           node{Node: n, et: et, logger: l},
		c:              n,
		groupSeriesIdx: make(map[models.GroupID]int),
		result:         new(influxql.Result),
	}
	et.registerOutput(hn.c.Endpoint, hn)
	hn.node.runF = hn.runOut
	hn.node.stopF = hn.stopOut
	return hn, nil
}

func (h *HTTPOutNode) Endpoint() string {
	return h.endpoint
}

func (h *HTTPOutNode) runOut([]byte) error {

	hndl := func(w http.ResponseWriter, req *http.Request) {
		h.mu.RLock()
		defer h.mu.RUnlock()

		if b, err := json.Marshal(h.result); err != nil {
			httpd.HttpError(
				w,
				err.Error(),
				true,
				http.StatusInternalServerError,
			)
		} else {
			_, _ = w.Write(b)
		}
	}

	p := path.Join("/tasks/", h.et.Task.ID, h.c.Endpoint)

	r := []httpd.Route{{
		Method:      "GET",
		Pattern:     p,
		HandlerFunc: hndl,
	}}

	h.endpoint = h.et.tm.HTTPDService.URL() + p
	func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.routes = r
	}()

	err := h.et.tm.HTTPDService.AddRoutes(r)
	if err != nil {
		return err
	}

	switch h.Wants() {
	case pipeline.StreamEdge:
		for p, ok := h.ins[0].NextPoint(); ok; p, ok = h.ins[0].NextPoint() {
			h.timer.Start()
			row := models.PointToRow(p)
			h.updateResultWithRow(p.Group, row)
			h.timer.Stop()
			for _, child := range h.outs {
				err := child.CollectPoint(p)
				if err != nil {
					return err
				}
			}
		}
	case pipeline.BatchEdge:
		for b, ok := h.ins[0].NextBatch(); ok; b, ok = h.ins[0].NextBatch() {
			h.timer.Start()
			row := models.BatchToRow(b)
			h.updateResultWithRow(b.Group, row)
			h.timer.Stop()
			for _, child := range h.outs {
				err := child.CollectBatch(b)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Update the result structure with a row.
func (h *HTTPOutNode) updateResultWithRow(group models.GroupID, row *imodels.Row) {
	h.mu.Lock()
	defer h.mu.Unlock()
	idx, ok := h.groupSeriesIdx[group]
	if !ok {
		idx = len(h.result.Series)
		h.groupSeriesIdx[group] = idx
		h.result.Series = append(h.result.Series, row)
	} else {
		h.result.Series[idx] = row
	}
}

func (h *HTTPOutNode) stopOut() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.et.tm.HTTPDService.DelRoutes(h.routes)
}
