package kapacitor

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"

	"github.com/influxdb/influxdb/influxql"
	imodels "github.com/influxdb/influxdb/models"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/tick"
)

// Number of previous states to remember when computing flapping percentage.
const defaultFlapHistory = 21

// The newest state change is weighted 'weightDiff' times more than oldest state change.
const weightDiff = 1.5

// Maximum weight applied to newest state change.
const maxWeight = 1.2

type AlertHandler func(ad AlertData)

type AlertLevel int

const (
	NoAlert AlertLevel = iota
	InfoAlert
	WarnAlert
	CritAlert
)

func (l AlertLevel) String() string {
	switch l {
	case NoAlert:
		return "noalert"
	case InfoAlert:
		return "INFO"
	case WarnAlert:
		return "WARNING"
	case CritAlert:
		return "CRITICAL"
	default:
		panic("unknown AlertLevel")
	}
}

func (l AlertLevel) MarshalText() ([]byte, error) {
	return []byte(l.String()), nil
}

type AlertData struct {
	Level AlertLevel      `json:"level"`
	Data  influxql.Result `json:"data"`
}

type AlertNode struct {
	node
	a        *pipeline.AlertNode
	endpoint string
	handlers []AlertHandler
	levels   []*tick.StatefulExpr
	history  []AlertLevel
	hIdx     int
	flapping bool
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
	if n.Log != "" {
		an.handlers = append(an.handlers, an.handleLog)
	}
	if len(n.Command) > 0 {
		an.handlers = append(an.handlers, an.handleExec)
	}
	// Parse level expressions
	an.levels = make([]*tick.StatefulExpr, CritAlert+1)
	if n.Info != nil {
		an.levels[InfoAlert] = tick.NewStatefulExpr(n.Info)
	}
	if n.Warn != nil {
		an.levels[WarnAlert] = tick.NewStatefulExpr(n.Warn)
	}
	if n.Crit != nil {
		an.levels[CritAlert] = tick.NewStatefulExpr(n.Crit)
	}
	// Configure flapping
	if n.UseFlapping {
		history := n.History
		if history == 0 {
			history = defaultFlapHistory
		}
		if history < 2 {
			return nil, errors.New("alert history count must be >= 2")
		}
		an.history = make([]AlertLevel, history)
		if n.FlapLow > 1 || n.FlapHigh > 1 {
			return nil, errors.New("alert flap thresholds are percentages and should be between 0 and 1")
		}
	}
	return
}

func (a *AlertNode) runAlert() error {
	switch a.Wants() {
	case pipeline.StreamEdge:
		for p, ok := a.ins[0].NextPoint(); ok; p, ok = a.ins[0].NextPoint() {
			l := a.determineLevel(p.Fields, p.Tags)
			if a.a.UseFlapping {
				a.updateFlapping(l)
				if a.flapping {
					continue
				}
			}
			if l > NoAlert {
				batch := models.Batch{
					Name:   p.Name,
					Group:  p.Group,
					Tags:   p.Tags,
					Points: []models.BatchPoint{models.BatchPointFromPoint(p)},
				}

				ad := AlertData{
					l,
					a.batchToResult(batch),
				}
				for _, h := range a.handlers {
					h(ad)
				}
			}
		}
	case pipeline.BatchEdge:
		for b, ok := a.ins[0].NextBatch(); ok; b, ok = a.ins[0].NextBatch() {
			triggered := false
			for _, p := range b.Points {
				l := a.determineLevel(p.Fields, p.Tags)
				if l > NoAlert {
					triggered = true
					if a.a.UseFlapping {
						a.updateFlapping(l)
						if a.flapping {
							break
						}
					}
					ad := AlertData{l, a.batchToResult(b)}
					for _, h := range a.handlers {
						h(ad)
					}
					break
				}
			}
			if !triggered && a.a.UseFlapping {
				a.updateFlapping(NoAlert)
			}
		}
	}
	return nil
}

func (a *AlertNode) determineLevel(fields models.Fields, tags map[string]string) (level AlertLevel) {
	for l, se := range a.levels {
		if se == nil {
			continue
		}
		if pass, err := EvalPredicate(se, fields, tags); pass {
			level = AlertLevel(l)
		} else if err != nil {
			a.logger.Println("E! error evaluating expression:", err)
			return
		} else {
			return
		}
	}
	return
}

func (a *AlertNode) batchToResult(b models.Batch) influxql.Result {
	row := models.BatchToRow(b)
	r := influxql.Result{
		Series: imodels.Rows{row},
	}
	return r
}

func (a *AlertNode) updateFlapping(level AlertLevel) {
	a.history[a.hIdx] = level
	a.hIdx = (a.hIdx + 1) % len(a.history)

	l := len(a.history)
	changes := 0.0
	weight := (maxWeight / weightDiff)
	step := (maxWeight - weight) / float64(l-1)
	for i := 1; i < l; i++ {
		// get current index
		c := (i + a.hIdx) % l
		// get previous index
		p := c - 1
		// check for wrap around
		if p < 0 {
			p = l - 1
		}
		if a.history[c] != a.history[p] {
			changes += weight
		}
		weight += step
	}

	p := changes / float64(l-1)

	if a.flapping && p < a.a.FlapLow {
		a.flapping = false
	} else if !a.flapping && p > a.a.FlapHigh {
		a.flapping = true
	}
}

func (a *AlertNode) handlePost(ad AlertData) {
	b, err := json.Marshal(ad)
	if err != nil {
		a.logger.Println("E! failed to marshal alert data json", err)
		return
	}
	buf := bytes.NewBuffer(b)
	_, err = http.Post(a.a.Post, "application/json", buf)
	if err != nil {
		a.logger.Println("E! failed to POST batch", err)
	}
}

func (a *AlertNode) handleEmail(ad AlertData) {
	b, err := json.Marshal(ad)
	if err != nil {
		a.logger.Println("E! failed to marshal alert data json", err)
		return
	}
	if a.et.tm.SMTPService != nil {
		a.et.tm.SMTPService.SendMail(a.a.From, a.a.ToList, a.a.Subject, string(b))
	} else {
		a.logger.Println("W! smtp service not enabled, cannot send email.")
	}
}

func (a *AlertNode) handleLog(ad AlertData) {
	b, err := json.Marshal(ad)
	if err != nil {
		a.logger.Println("E! failed to marshal alert data json", err)
		return
	}
	f, err := os.OpenFile(a.a.Log, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		a.logger.Println("E! failed to open file for alert logging", err)
		return
	}
	defer f.Close()
	n, err := f.Write(b)
	if n != len(b) || err != nil {
		a.logger.Println("E! failed to write to file", err)
	}
	n, err = f.Write([]byte("\n"))
	if n != 1 || err != nil {
		a.logger.Println("E! failed to write to file", err)
	}
}

func (a *AlertNode) handleExec(ad AlertData) {
	b, err := json.Marshal(ad)
	if err != nil {
		a.logger.Println("E! failed to marshal alert data json", err)
		return
	}
	cmd := exec.Command(a.a.Command[0], a.a.Command[1:]...)
	cmd.Stdin = bytes.NewBuffer(b)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err = cmd.Run()
	if err != nil {
		a.logger.Println("E! error running alert command:", err, out.String())
		return
	}
}
