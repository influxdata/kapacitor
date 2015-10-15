package kapacitor

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

type AlertHandler func(batch models.Batch)

type AlertNode struct {
	node
	a        *pipeline.AlertNode
	endpoint string
	handlers []AlertHandler
}

// Create a new  AlertNode which caches the most recent item and exposes it over the HTTP API.
func newAlertNode(et *ExecutingTask, n *pipeline.AlertNode) (an *AlertNode, err error) {
	an = &AlertNode{
		node: node{Node: n, et: et},
		a:    n,
	}
	an.node.runF = an.runAlert
	// Construct alert handlers
	an.handlers = make([]AlertHandler, 0)
	if n.Post != "" {
		an.handlers = append(an.handlers, an.handlePost)
	}
	if n.From != "" && len(n.ToList) != 0 {
		an.handlers = append(an.handlers, an.handleEmail)
	}
	return
}

func (a *AlertNode) runAlert() error {
	switch a.Wants() {
	case pipeline.StreamEdge:
		for p, ok := a.ins[0].NextPoint(); ok; p, ok = a.ins[0].NextPoint() {
			batch := models.Batch{
				Name:   p.Name,
				Group:  p.Group,
				Tags:   p.Tags,
				Points: []models.TimeFields{{Time: p.Time, Fields: p.Fields}},
			}
			for _, h := range a.handlers {
				h(batch)
			}
		}
	case pipeline.BatchEdge:
		for b, ok := a.ins[0].NextBatch(); ok; b, ok = a.ins[0].NextBatch() {
			for _, h := range a.handlers {
				h(b)
			}
		}
	}
	return nil
}

func (a *AlertNode) handlePost(batch models.Batch) {
	b, err := json.Marshal(batch)
	if err != nil {
		a.logger.Println("E! failed to marshal batch json", err)
		return
	}
	buf := bytes.NewBuffer(b)
	_, err = http.Post(a.a.Post, "application/json", buf)
	if err != nil {
		a.logger.Println("E! failed to POST batch", err)
	}
}

func (a *AlertNode) handleEmail(batch models.Batch) {
	b, err := json.Marshal(batch)
	if err != nil {
		a.logger.Println("E! failed to marshal batch json", err)
		return
	}
	if a.et.tm.SMTPService != nil {
		a.et.tm.SMTPService.SendMail(a.a.From, a.a.ToList, a.a.Subject, string(b))
	} else {
		a.logger.Println("W! smtp service not enabled, cannot send email.")
	}
}
