package kapacitor

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"

	"bytes"
	"context"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/services/httppost"
	"github.com/pkg/errors"
)

type HTTPPostNode struct {
	node
	c        *pipeline.HTTPPostNode
	endpoint *httppost.Endpoint
	mu       sync.RWMutex
	timeout  time.Duration
	hc       *http.Client
}

// Create a new  HTTPPostNode which submits received items via POST to an HTTP endpoint
func newHTTPPostNode(et *ExecutingTask, n *pipeline.HTTPPostNode, d NodeDiagnostic) (*HTTPPostNode, error) {

	hn := &HTTPPostNode{
		node:    node{Node: n, et: et, diag: d},
		c:       n,
		timeout: n.Timeout,
	}

	// Should only ever be 0 or 1 from validation of n
	if len(n.URLs) == 1 {
		e := httppost.NewEndpoint(n.URLs[0], nil, httppost.BasicAuth{}, nil, nil)
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
	code := g.n.doPost(row)
	if g.n.c.CodeField != "" {
		//Add code to all points
		batch = batch.ShallowCopy()
		points := make([]edge.BatchPointMessage, len(batch.Points()))
		for i, bp := range batch.Points() {
			fields := bp.Fields().Copy()
			fields[g.n.c.CodeField] = int64(code)
			points[i] = edge.NewBatchPointMessage(
				fields,
				bp.Tags(),
				bp.Time(),
			)
		}
		batch.SetPoints(points)
	}
	return batch, nil
}

func (g *httpPostGroup) Point(p edge.PointMessage) (edge.Message, error) {
	row := p.ToRow()
	code := g.n.doPost(row)
	if g.n.c.CodeField != "" {
		//Add code to point
		p = p.ShallowCopy()
		fields := p.Fields().Copy()
		fields[g.n.c.CodeField] = int64(code)
		p.SetFields(fields)
	}
	return p, nil
}

func (g *httpPostGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (g *httpPostGroup) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (g *httpPostGroup) Done() {}

func (n *HTTPPostNode) doPost(row *models.Row) int {
	resp, err := n.postRow(row)
	if err != nil {
		n.diag.Error("failed to POST data", err)
		return 0
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		var err error
		if n.c.CaptureResponseFlag {
			var body []byte
			body, err = ioutil.ReadAll(resp.Body)
			if err == nil {
				// Use the body content as the error
				err = errors.New(string(body))
			}
		} else {
			err = errors.New("unknown error, use .captureResponse() to capture the HTTP response")
		}
		n.diag.Error("POST returned non 2xx status code", err, keyvalue.KV("code", strconv.Itoa(resp.StatusCode)))
	}
	return resp.StatusCode
}

func (n *HTTPPostNode) postRow(row *models.Row) (*http.Response, error) {
	body := new(bytes.Buffer)

	var contentType string
	if n.endpoint.RowTemplate() != nil {
		mr := newMappedRow(row)
		err := n.endpoint.RowTemplate().Execute(body, mr)
		if err != nil {
			return nil, errors.Wrap(err, "failed to execute template")
		}
	} else {
		result := new(models.Result)
		result.Series = []*models.Row{row}
		err := json.NewEncoder(body).Encode(result)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal row data json")
		}
		contentType = "application/json"
	}

	req, err := n.endpoint.NewHTTPRequest(body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal row data json")
	}

	// Set content type and other headers
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for k, v := range n.c.Headers {
		req.Header.Set(k, v)
	}

	// Set timeout
	if n.timeout > 0 {
		ctx, cancel := context.WithTimeout(req.Context(), n.timeout)
		defer cancel()
		req = req.WithContext(ctx)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

type mappedRow struct {
	Name   string
	Tags   map[string]string
	Values []map[string]interface{}
}

func newMappedRow(row *models.Row) *mappedRow {
	values := make([]map[string]interface{}, len(row.Values))
	for i, v := range row.Values {
		values[i] = make(map[string]interface{}, len(row.Columns))
		for c, col := range row.Columns {
			values[i][col] = v[c]
		}
	}
	return &mappedRow{
		Name:   row.Name,
		Tags:   row.Tags,
		Values: values,
	}
}
