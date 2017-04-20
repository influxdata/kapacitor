package kapacitor

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/influxdata/kapacitor/bufpool"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

type HTTPPostNode struct {
	node
	c   *pipeline.HTTPPostNode
	url string
	mu  sync.RWMutex
	bp  *bufpool.Pool
}

// Create a new  HTTPPostNode which submits received items via POST to an HTTP endpoint
func newHTTPPostNode(et *ExecutingTask, n *pipeline.HTTPPostNode, l *log.Logger) (*HTTPPostNode, error) {
	hn := &HTTPPostNode{
		node: node{Node: n, et: et, logger: l},
		c:    n,
		bp:   bufpool.New(),
		url:  n.Url,
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
		h.logger.Printf("E! failed to marshal row data json: %v", err)
		return
	}

	resp, err := http.Post(h.url, "application/json", body)
	if err != nil {
		h.logger.Printf("E! failed to POST row data: %v", err)
		return
	}
	resp.Body.Close()
}
