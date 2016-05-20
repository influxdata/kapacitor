package pipeline

import (
	"testing"
	"time"

	"github.com/influxdata/kapacitor/tick/stateful"
	"github.com/stretchr/testify/assert"
)

type deadman struct {
	interval  time.Duration
	threshold float64
	id        string
	message   string
	global    bool
}

func (d deadman) Interval() time.Duration { return d.interval }
func (d deadman) Threshold() float64      { return d.threshold }
func (d deadman) Id() string              { return d.id }
func (d deadman) Message() string         { return d.message }
func (d deadman) Global() bool            { return d.global }

func TestTICK_To_Pipeline_MultiLine(t *testing.T) {
	var tickScript = `
var w = stream
	|from()
	|window()

w.period(10s)
w.every(1s)
`

	d := deadman{}

	scope := stateful.NewScope()
	p, err := CreatePipeline(tickScript, StreamEdge, scope, d, nil)
	if err != nil {
		t.Fatal(err)
	}
	if p == nil {
		t.Fatal("unexpected pipeline, got nil")
	}
	if exp, got := 1, len(p.sources); exp != got {
		t.Errorf("unexpected number of pipeline sources: exp %d got %d", exp, got)
	}
	if exp, got := 1, len(p.sources[0].Children()); exp != got {
		t.Errorf("unexpected number of source0 children: exp %d got %d", exp, got)
	}
	sn, ok := p.sources[0].Children()[0].(*FromNode)
	if !ok {
		t.Fatalf("unexpected node type: exp FromNode got %T", p.sources[0].Children()[0])
	}
	w, ok := sn.Children()[0].(*WindowNode)
	if !ok {
		t.Fatalf("unexpected node type: exp WindowNode got %T", sn.Children()[0])
	}

	if exp, got := time.Duration(10)*time.Second, w.Period; exp != got {
		t.Errorf("unexpected window period exp %v got %v", exp, got)
	}
	if exp, got := time.Duration(1)*time.Second, w.Every; exp != got {
		t.Errorf("unexpected window every exp %v got %v", exp, got)
	}
}

func TestPipelineSort(t *testing.T) {
	assert := assert.New(t)

	p := &Pipeline{}

	p1 := &node{p: p}
	p2 := &node{p: p}
	p3 := &node{p: p}
	p4 := &node{p: p}
	p5 := &node{p: p}

	p.addSource(p1)

	p1.linkChild(p2)
	p2.linkChild(p3)
	p1.linkChild(p4)
	p4.linkChild(p5)
	p5.linkChild(p3)

	p.sort()

	sorted := []Node{
		p1,
		p4,
		p5,
		p2,
		p3,
	}

	assert.Equal(sorted, p.sorted)
}
