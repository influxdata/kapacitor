package pipeline

import (
	"testing"
	"time"

	"github.com/influxdata/kapacitor/tick"
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
	assert := assert.New(t)

	var tickScript = `
var w = stream.window()
w.period(10s)
w.every(1s)
`

	d := deadman{}

	scope := tick.NewScope()
	p, err := CreatePipeline(tickScript, StreamEdge, scope, d)
	assert.Nil(err)
	assert.NotNil(p)
	assert.Equal(1, len(p.sources[0].Children()))
	w, ok := p.sources[0].Children()[0].(*WindowNode)
	if assert.True(ok) {
		assert.Equal(time.Duration(10)*time.Second, w.Period)
		assert.Equal(time.Duration(1)*time.Second, w.Every)
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
