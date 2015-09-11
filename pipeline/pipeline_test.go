package pipeline

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDSL2PipelineMultiLine(t *testing.T) {
	assert := assert.New(t)

	var dslScript = `
var w = stream.window();
w.period(10s);
w.every(1s);
`

	s, err := CreatePipeline(dslScript)
	assert.Nil(err)
	assert.NotNil(s)
	assert.Equal(1, s.NumChildren())
	w := s.Children()[0]
	assert.Equal(time.Duration(10)*time.Second, w.Period)
	assert.Equal(time.Duration(1)*time.Second, w.Every)

}
func TestDSL2PipelineSingleLine(t *testing.T) {
	assert := assert.New(t)
	var dslScript = `
stream.window().options(30s, 4s);
`
	s, err := CreatePipeline(dslScript)
	if !assert.Nil(err) {
		t.FailNow()
	}
	assert.NotNil(s)
	assert.Equal(1, s.NumChildren())
	w := s.Children()[0]
	assert.Equal(time.Duration(30)*time.Second, w.Period)
	assert.Equal(time.Duration(4)*time.Second, w.Every)
}
