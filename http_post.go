package kapacitor

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/influxdata/kapacitor/bufpool"
	"github.com/influxdata/kapacitor/edge"
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

func (n *HTTPPostNode) runPost([]byte) error {
	consumer := edge.NewGroupedConsumer(
		n.ins[0],
		n,
	)
	n.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())

	return consumer.Consume()

}

func (n *HTTPPostNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	g := &httpPostGroup{
		n:      n,
		buffer: new(edge.BatchBuffer),
	}
	return edge.NewReceiverFromForwardReceiverWithStats(
		n.outs,
		edge.NewTimedForwardReceiver(n.timer, g),
	), nil
}

type httpPostGroup struct {
	n      *HTTPPostNode
	buffer *edge.BatchBuffer
}

func (g *httpPostGroup) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	return nil, g.buffer.BeginBatch(begin)
}

func (g *httpPostGroup) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	return nil, g.buffer.BatchPoint(bp)
}

func (g *httpPostGroup) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return g.BufferedBatch(g.buffer.BufferedBatchMessage(end))
}

func (g *httpPostGroup) BufferedBatch(batch edge.BufferedBatchMessage) (edge.Message, error) {
	row := batch.ToRow()
	g.n.postRow(row)
	return batch, nil
}

func (g *httpPostGroup) Point(p edge.PointMessage) (edge.Message, error) {
	row := p.ToRow()
	g.n.postRow(row)
	return p, nil
}

func (g *httpPostGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (g *httpPostGroup) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}

func (n *HTTPPostNode) postRow(row *models.Row) {
	result := new(models.Result)
	result.Series = []*models.Row{row}

	body := n.bp.Get()
	defer n.bp.Put(body)
	err := json.NewEncoder(body).Encode(result)
	if err != nil {
		n.incrementErrorCount()
		n.logger.Printf("E! failed to marshal row data json: %v", err)
		return
	}
	req, err := n.endpoint.NewHTTPRequest(body)
	if err != nil {
		n.incrementErrorCount()
		n.logger.Printf("E! failed to marshal row data json: %v", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range n.c.Headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		n.incrementErrorCount()
		n.logger.Printf("E! failed to POST row data: %v", err)
		return
	}
	resp.Body.Close()
}
