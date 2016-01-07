package kapacitor

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/stretchr/testify/assert"
)

var logger = log.New(os.Stderr, "[window] ", log.LstdFlags|log.Lshortfile)

func TestWindowBuffer(t *testing.T) {
	assert := assert.New(t)

	buf := &windowBuffer{logger: logger}

	size := 100

	// fill buffer
	for i := 1; i <= size; i++ {

		t := time.Unix(int64(i), 0)
		p := models.Point{
			Time: t,
		}
		buf.insert(p)

		assert.Equal(i, buf.size)
		assert.Equal(0, buf.start)
		assert.Equal(i, buf.stop)
	}

	// purge entire buffer
	for i := 0; i <= size; i++ {

		oldest := time.Unix(int64(i+1), 0)
		buf.purge(oldest)

		assert.Equal(size-i, buf.size, "i: %d", i)
		assert.Equal(i, buf.start, "i: %d", i)
		assert.Equal(size, buf.stop, "i: %d", i)

		batch := buf.batch()
		if assert.Equal(size-i, len(batch.Points)) {
			for _, p := range batch.Points {
				assert.True(!p.Time.Before(oldest), "Point %s is not after oldest time %s", p.Time, oldest)
			}
		}
	}

	assert.Equal(0, buf.size)

	// fill buffer again
	oldest := time.Unix(int64(size), 0)
	for i := 1; i <= size*2; i++ {

		t := time.Unix(int64(i+size), 0)
		p := models.Point{
			Time: t,
		}
		buf.insert(p)

		assert.Equal(i, buf.size)

		batch := buf.batch()
		if assert.Equal(i, len(batch.Points)) {
			for _, p := range batch.Points {
				if assert.NotNil(p, "i:%d", i) {
					assert.True(!p.Time.Before(oldest), "Point %s is not after oldest time %s", p.Time, oldest)
				}
			}
		}
	}
}
