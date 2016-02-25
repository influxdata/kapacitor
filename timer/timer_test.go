package timer

import (
	"math"
	"math/rand"
	"testing"
)

func TestMovAvg(t *testing.T) {
	count := 100
	ma := newMovAvg(count)
	for i := 0; i < count; i++ {
		avg := ma.update(1)
		if got, exp := avg, 1.0; got != exp {
			t.Fatalf("unexpected movavg: got %f, exp %f", got, exp)
		}
	}
	c := float64(count)
	for i := 0; i < count; i++ {
		avg := ma.update(2)
		f := float64(i + 1)
		if got, exp := avg, ((c-f)+2.0*f)/c; math.Abs(got-exp) > 1e-8 {
			t.Fatalf("unexpected movavg i: %d got %f, exp %f", i, got, exp)
		}
	}
}

func TestMovAvgAccuracy(t *testing.T) {
	count := 10000

	avg := func(data []float64) float64 {
		sum := 0.0
		for _, v := range data {
			sum += v
		}
		return sum / float64(len(data))
	}

	r := rand.New(rand.NewSource(42))

	data := make([]float64, count)
	for i := 0; i < count; i++ {
		data[i] = r.Float64()
	}
	expAvg := avg(data)
	ma := newMovAvg(count)
	gotAvg := 0.0
	for _, v := range data {
		gotAvg = ma.update(v)
	}

	if math.Abs(gotAvg-expAvg) > 1e-8 {
		t.Errorf("unexpected average value: got %f exp %f", gotAvg, expAvg)
	}

	size := count / 10
	ma = newMovAvg(size)

	expAvg = avg(data[count-size:])
	gotAvg = 0.0
	for _, v := range data {
		gotAvg = ma.update(v)
	}
	if math.Abs(gotAvg-expAvg) > 1e-8 {
		t.Errorf("unexpected moving average value: got %f exp %f", gotAvg, expAvg)
	}
}

func BenchmarkMovAvgUpdate(b *testing.B) {
	ma := newMovAvg(1000)
	for i := 0; i < b.N; i++ {
		ma.update(float64(i))
	}
}

type setter struct {
	value int64
}

func (s *setter) Set(v int64) {
	s.value = v
}

func TestSampling(t *testing.T) {
	sr := 0.1
	size := 10000
	rand.Seed(0)

	s := &setter{}
	tmr := New(sr, size, s).(*timer)

	for i := 0; i < size; i++ {
		tmr.Start()
		tmr.Stop()
	}

	count := sr * float64(size)
	err := 0.1
	if math.Abs(float64(tmr.avg.count)-count) > err*count {
		t.Errorf("unexpected numbers of samples taken got: %d exp: %d", int(tmr.avg.count), int(count))
	}

}

func BenchmarkTimerStartStopWorst(b *testing.B) {
	// Use 100% sample rate worst case
	sr := 1.0
	size := 1000
	s := &setter{}
	tmr := New(sr, size, s).(*timer)
	for i := 0; i < b.N; i++ {
		tmr.Start()
		tmr.Stop()
	}
}

func BenchmarkTimerStartPauseResumeStopWorst(b *testing.B) {
	// Use 100% sample rate worst case
	sr := 1.0
	size := 1000
	s := &setter{}
	tmr := New(sr, size, s).(*timer)
	for i := 0; i < b.N; i++ {
		tmr.Start()
		tmr.Pause()
		tmr.Resume()
		tmr.Stop()
	}
}

func BenchmarkTimerStartStopBest(b *testing.B) {
	// Use 0% sample rate best case
	sr := 0.0
	size := 1000
	s := &setter{}
	tmr := New(sr, size, s).(*timer)
	for i := 0; i < b.N; i++ {
		tmr.Start()
		tmr.Stop()
	}
}

func BenchmarkTimerStartPauseResumeStopBest(b *testing.B) {
	// Use 0% sample rate base case
	sr := 0.0
	size := 1000
	s := &setter{}
	tmr := New(sr, size, s).(*timer)
	for i := 0; i < b.N; i++ {
		tmr.Start()
		tmr.Pause()
		tmr.Resume()
		tmr.Stop()
	}
}
