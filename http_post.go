package kapacitor

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/influxdata/kapacitor/bufpool"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/services/httppost"
)

type HTTPPostNode struct {
	node
	c        *pipeline.HTTPPostNode
	endpoint *httppost.Endpoint
	mu       sync.RWMutex
	bp       *bufpool.Pool
}

// Create a new  HTTPPostNode which submits received items via POST to an HTTP endpoint
func newHTTPPostNode(et *ExecutingTask, n *pipeline.HTTPPostNode, l *log.Logger) (*HTTPPostNode, error) {

	hn := &HTTPPostNode{
		node: node{Node: n, et: et, logger: l},
		c:    n,
		bp:   bufpool.New(),
	}

	// Should only ever be 0 or 1 from validation of n
	if len(n.URLs) == 1 {
		e := httppost.NewEndpoint(n.URLs[0], nil, httppost.BasicAuth{})
		hn.endpoint = e
	}

	// Should only ever be 0 or 1 from validation of n
	if len(n.Endpoints) == 1 {
		endpointName := n.Endpoints[0]
		e, ok := et.tm.HTTPPostService.Endpoint(endpointName)
		if !ok {
			return nil, fmt.Errorf("endpoint '%s' does not exist", endpointName)
		}
		hn.endpoint = e
	}

	hn.node.runF = hn.runPost
	return hn, nil
}

func (h *HTTPPostNode) runPost([]byte) error {
	switch h.Wants() {
	case pipeline.StreamEdge:
		for p, ok := h.ins[0].NextPoint(); ok; p, ok = h.ins[0].NextPoint() {
			h.timer.Start()
			row := models.PointToRow(p)
			h.postRow(p.Group, row)
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
			h.postRow(b.Group, row)
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
func (h *HTTPPostNode) postRow(group models.GroupID, row *models.Row) {
	result := new(models.Result)
	result.Series = []*models.Row{row}

	body := h.bp.Get()
	defer h.bp.Put(body)
	err := json.NewEncoder(body).Encode(result)
	if err != nil {
		h.incrementErrorCount()
		h.logger.Printf("E! failed to marshal row data json: %v", err)
		return
	}
	req, err := h.endpoint.NewHTTPRequest(body)
	if err != nil {
		h.incrementErrorCount()
		h.logger.Printf("E! failed to marshal row data json: %v", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range h.c.Headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.incrementErrorCount()
		h.logger.Printf("E! failed to POST row data: %v", err)
		return
	}
	resp.Body.Close()

}
