package kapacitor

import (
	"testing"
	"time"

	"github.com/influxdb/kapacitor/models"
	"github.com/stretchr/testify/assert"
)

func TestWindowBuffer(t *testing.T) {
	assert := assert.New(t)

	buf := &windowBuffer{}

	size := 100

	// fill buffer
	for i := 1; i <= size; i++ {

		t := time.Unix(int64(i), 0)
		p := models.NewPoint("TestWindowBuffer", models.NilGroup, nil, nil, t)
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

		points := buf.points()
		if assert.Equal(size-i, len(points)) {
			for _, p := range points {
				assert.True(!p.Time.Before(oldest), "Point %s is not after oldest time %s", p.Time, oldest)
			}
		}
	}

	assert.Equal(0, buf.size)

	// fill buffer again
	for i := 1; i <= size; i++ {

		t := time.Unix(int64(i), 0)
		p := models.NewPoint("TestWindowBuffer", models.NilGroup, nil, nil, t)
		buf.insert(p)

		assert.Equal(i, buf.size)

		points := buf.points()
		assert.Equal(i, len(points))
	}
}
