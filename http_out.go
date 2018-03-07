package kapacitor

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"sync"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/services/httpd"
)

type HTTPOutNode struct {
	node
	c *pipeline.HTTPOutNode

	endpoint string

	mu      sync.RWMutex
	routes  []httpd.Route
	result  *models.Result
	indexes []*httpOutGroup
}

// Create a new  HTTPOutNode which caches the most recent item and exposes it over the HTTP API.
func newHTTPOutNode(et *ExecutingTask, n *pipeline.HTTPOutNode, d NodeDiagnostic) (*HTTPOutNode, error) {
	hn := &HTTPOutNode{
		node:   node{Node: n, et: et, diag: d},
		c:      n,
		result: new(models.Result),
	}
	et.registerOutput(hn.c.Endpoint, hn)
	hn.node.runF = hn.runOut
	hn.node.stopF = hn.stopOut
	return hn, nil
}

func (n *HTTPOutNode) Endpoint() string {
	return n.endpoint
}

func (n *HTTPOutNode) runOut([]byte) error {
	hndl := func(w http.ResponseWriter, req *http.Request) {
		n.mu.RLock()
		defer n.mu.RUnlock()

		if b, err := json.Marshal(n.result); err != nil {
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

	p := path.Join("/tasks/", n.et.Task.ID, n.c.Endpoint)

	r := []httpd.Route{{
		Method:      "GET",
		Pattern:     p,
		HandlerFunc: hndl,
	}}

	n.endpoint = n.et.tm.HTTPDService.URL() + p
	n.mu.Lock()
	n.routes = r
	n.mu.Unlock()

	err := n.et.tm.HTTPDService.AddRoutes(r)
	if err != nil {
		return err
	}

	consumer := edge.NewGroupedConsumer(
		n.ins[0],
		n,
	)
	n.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())

	return consumer.Consume()
}

// Update the result structure with a row.
func (n *HTTPOutNode) updateResultWithRow(idx int, row *models.Row) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if idx >= len(n.result.Series) {
		n.diag.Error("index out of range for row update",
			fmt.Errorf("index %v is larger than number of series %v", idx, len(n.result.Series)))
		return
	}
	n.result.Series[idx] = row
}

func (n *HTTPOutNode) stopOut() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.et.tm.HTTPDService.DelRoutes(n.routes)
}

func (n *HTTPOutNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	return edge.NewReceiverFromForwardReceiverWithStats(
		n.outs,
		edge.NewTimedForwardReceiver(n.timer, n.newGroup(group.ID)),
	), nil
}

func (n *HTTPOutNode) newGroup(groupID models.GroupID) *httpOutGroup {
	n.mu.Lock()
	defer n.mu.Unlock()

	idx := len(n.result.Series)
	n.result.Series = append(n.result.Series, nil)
	g := &httpOutGroup{
		n:      n,
		idx:    idx,
		buffer: new(edge.BatchBuffer),
	}
	n.indexes = append(n.indexes, g)
	return g
}
func (n *HTTPOutNode) deleteGroup(idx int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, g := range n.indexes[idx+1:] {
		g.idx--
	}
	n.indexes = append(n.indexes[0:idx], n.indexes[idx+1:]...)
	n.result.Series = append(n.result.Series[0:idx], n.result.Series[idx+1:]...)
}

type httpOutGroup struct {
	n      *HTTPOutNode
	id     models.GroupID
	idx    int
	buffer *edge.BatchBuffer
}

func (g *httpOutGroup) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	return nil, g.buffer.BeginBatch(begin)
}

func (g *httpOutGroup) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	return nil, g.buffer.BatchPoint(bp)
}

func (g *httpOutGroup) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return g.BufferedBatch(g.buffer.BufferedBatchMessage(end))
}

func (g *httpOutGroup) BufferedBatch(batch edge.BufferedBatchMessage) (edge.Message, error) {
	row := batch.ToRow()
	g.n.updateResultWithRow(g.idx, row)
	return batch, nil
}

func (g *httpOutGroup) Point(p edge.PointMessage) (edge.Message, error) {
	row := p.ToRow()
	g.n.updateResultWithRow(g.idx, row)
	return p, nil
}

func (g *httpOutGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (g *httpOutGroup) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	g.n.deleteGroup(g.idx)
	return d, nil
}
func (g *httpOutGroup) Done() {}
