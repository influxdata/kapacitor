package pipeline

import (
	"testing"
	"time"

	"github.com/influxdata/kapacitor/tick"
	"github.com/stretchr/testify/assert"
)

func TestTICK_To_Pipeline_MultiLine(t *testing.T) {
	assert := assert.New(t)

	var tickScript = `
var w = stream.window()
w.period(10s)
w.every(1s)
`

	scope := tick.NewScope()
	p, err := CreatePipeline(tickScript, StreamEdge, scope)
	assert.Nil(err)
	assert.NotNil(p)
	assert.Equal(1, len(p.Source.Children()))
	w, ok := p.Source.Children()[0].(*WindowNode)
	if assert.True(ok) {
		assert.Equal(time.Duration(10)*time.Second, w.Period)
		assert.Equal(time.Duration(1)*time.Second, w.Every)
	}

}

func TestPipelineSort(t *testing.T) {
	assert := assert.New(t)

	p1 := &node{}
	p2 := &node{}
	p3 := &node{}
	p4 := &node{}
	p5 := &node{}

	p1.linkChild(p2)
	p2.linkChild(p3)
	p1.linkChild(p4)
	p4.linkChild(p5)
	p5.linkChild(p3)

	p := &Pipeline{
		Source: p1,
	}

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
