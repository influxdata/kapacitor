package uuid

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

/****************
 * Date: 15/02/14
 * Time: 12:19 PM
 ***************/

func TestEpoch(t *testing.T) {
	assert.True(t, gregorianToUNIXOffset == 0x01B21DD213814000)
	assert.True(t, gregorianToUNIXOffset == 122192928000000000)
}

func TestTimestampSize(t *testing.T) {
	assert.True(t, Now() > gregorianToUNIXOffset)
}

func TestNowToTime(t *testing.T) {
	now := time.Now()
	nano := now.UnixNano()
	convertedNano := Convert(now).Time().UnixNano()
	assert.True(t, nano/100*100 == convertedNano, "Times do not match %d %d", nano, convertedNano)
}

func TestTimestampToTimeShouldBeUTC(t *testing.T) {
	assert.True(t, Now().Time().Location() == time.UTC)
}

func TestDuplicateTimestampsSingleRoutine(t *testing.T) {
	size := defaultSpinResolution * 10

	spin := spinner{}
	spin.Resolution = defaultSpinResolution

	times := make([]Timestamp, size)

	for i := 0; i < size; i++ {
		times[i] = spin.next()
	}

	for j := size - 1; j >= 0; j-- {
		for k := 0; k < size; k++ {
			if k == j {
				continue
			}
			assert.NotEqual(t, "Timestamps should never be equal", times[j], times[k])
		}
	}
}

// Tests that the schedule saves properly when uuid are called in go routines
func TestDuplicateTimestampsMultipleRoutine(t *testing.T) {

	size := defaultSpinResolution * 10
	waitSize := 3

	spin := spinner{}
	spin.Resolution = defaultSpinResolution

	times := make([]Timestamp, size)

	var wg sync.WaitGroup
	wg.Add(waitSize)
	mutex := &sync.Mutex{}

	var index int32

	for i := 0; i < waitSize; i++ {
		go func() {
			defer wg.Done()
			for {
				mutex.Lock()
				j := atomic.LoadInt32(&index)
				atomic.AddInt32(&index, 1)
				if j >= int32(size) {
					mutex.Unlock()
					break
				}
				times[j] = spin.next()
				mutex.Unlock()
			}
		}()
	}
	wg.Wait()

	for j := size - 1; j >= 0; j-- {
		for k := 0; k < size; k++ {
			if k == j {
				continue
			}
			assert.NotEqual(t, "Timestamps should never be equal", times[j], times[k])
		}
	}

}
