package kapacitor

import (
	"encoding/json"
	"net/http"
	"path"

	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/services/httpd"
)

type HTTPOutNode struct {
	node
	c        *pipeline.HTTPOutNode
	windows  map[models.GroupID][]*models.Point
	endpoint string
	routes   []httpd.Route
}

// Create a new  HTTPOutNode which caches the most recent item and exposes it over the HTTP API.
func newHTTPOutNode(et *ExecutingTask, n *pipeline.HTTPOutNode) (*HTTPOutNode, error) {
	hn := &HTTPOutNode{
		node:    node{Node: n, et: et},
		c:       n,
		windows: make(map[models.GroupID][]*models.Point),
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
		if b, err := json.Marshal(h.windows); err != nil {
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
		for p := h.ins[0].NextPoint(); p != nil; p = h.ins[0].NextPoint() {
			h.windows[p.Group] = []*models.Point{p}
		}
	case pipeline.BatchEdge:
		for w := h.ins[0].NextBatch(); w != nil; w = h.ins[0].NextBatch() {
			if len(w) > 0 {
				h.windows[w[0].Group] = w
			}
		}
	}

	return nil
}

func (h *HTTPOutNode) stopOut() {
	h.et.tm.HTTPDService.DelRoutes(h.routes)
}
