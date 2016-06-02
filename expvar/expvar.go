// This package is a fork of the golang expvar expvar.Var types.
// Adding extra support for deleting and accessing raw typed values.
package expvar

import (
	"bytes"
	"expvar"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

type IntVar interface {
	IntValue() int64
}

type FloatVar interface {
	FloatValue() float64
}

type StringVar interface {
	StringValue() string
}

// Int is a 64-bit integer variable that satisfies the expvar.Var interface.
type Int struct {
	i int64
}

func (v *Int) String() string {
	return strconv.FormatInt(v.IntValue(), 10)
}

func (v *Int) Add(delta int64) {
	atomic.AddInt64(&v.i, delta)
}

func (v *Int) Set(value int64) {
	atomic.StoreInt64(&v.i, value)
}

func (v *Int) IntValue() int64 {
	return atomic.LoadInt64(&v.i)
}

// Float is a 64-bit float variable that satisfies the expvar.Var interface.
type Float struct {
	f uint64
}

func (v *Float) String() string {
	return strconv.FormatFloat(v.FloatValue(), 'g', -1, 64)
}

func (v *Float) FloatValue() float64 {
	return math.Float64frombits(atomic.LoadUint64(&v.f))
}

// Add adds delta to v.
func (v *Float) Add(delta float64) {
	for {
		cur := atomic.LoadUint64(&v.f)
		curVal := math.Float64frombits(cur)
		nxtVal := curVal + delta
		nxt := math.Float64bits(nxtVal)
		if atomic.CompareAndSwapUint64(&v.f, cur, nxt) {
			return
		}
	}
}

// Set sets v to value.
func (v *Float) Set(value float64) {
	atomic.StoreUint64(&v.f, math.Float64bits(value))
}

// Map is a string-to-expvar.Var map variable that satisfies the expvar.Var interface.
type Map struct {
	mu sync.RWMutex
	m  map[string]expvar.Var
}

func (v *Map) String() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	var b bytes.Buffer
	fmt.Fprintf(&b, "{")
	first := true
	v.doLocked(func(kv expvar.KeyValue) {
		if !first {
			fmt.Fprintf(&b, ", ")
		}
		fmt.Fprintf(&b, "%q: %v", kv.Key, kv.Value)
		first = false
	})
	fmt.Fprintf(&b, "}")
	return b.String()
}

func (v *Map) Init() *Map {
	v.m = make(map[string]expvar.Var)
	return v
}

func (v *Map) Get(key string) expvar.Var {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.m[key]
}

func (v *Map) Set(key string, av expvar.Var) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.m[key] = av
}

func (v *Map) Delete(key string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.m, key)
}

func (v *Map) Add(key string, delta int64) {
	v.mu.RLock()
	av, ok := v.m[key]
	v.mu.RUnlock()
	if !ok {
		// check again under the write lock
		v.mu.Lock()
		av, ok = v.m[key]
		if !ok {
			av = new(Int)
			v.m[key] = av
		}
		v.mu.Unlock()
	}

	// Add to Int; ignore otherwise.
	if iv, ok := av.(*Int); ok {
		iv.Add(delta)
	}
}

// AddFloat adds delta to the *Float value stored under the given map key.
func (v *Map) AddFloat(key string, delta float64) {
	v.mu.RLock()
	av, ok := v.m[key]
	v.mu.RUnlock()
	if !ok {
		// check again under the write lock
		v.mu.Lock()
		av, ok = v.m[key]
		if !ok {
			av = new(Float)
			v.m[key] = av
		}
		v.mu.Unlock()
	}

	// Add to Float; ignore otherwise.
	if iv, ok := av.(*Float); ok {
		iv.Add(delta)
	}
}

// Do calls f for each entry in the map.
// The map is locked during the iteration,
// but existing entries may be concurrently updated.
func (v *Map) Do(f func(expvar.KeyValue)) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	v.doLocked(f)
}

// DoSorted calls f for each entry in the map in sorted order.
// The map is locked during the iteration,
// but existing entries may be concurrently updated.
func (v *Map) DoSorted(f func(expvar.KeyValue)) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	keys := make([]string, len(v.m))
	i := 0
	for key := range v.m {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	for _, k := range keys {
		f(expvar.KeyValue{k, v.m[k]})
	}
}

// doLocked calls f for each entry in the map.
// v.mu must be held for reads.
func (v *Map) doLocked(f func(expvar.KeyValue)) {
	for k, v := range v.m {
		f(expvar.KeyValue{k, v})
	}
}

// String is a string variable, and satisfies the expvar.Var interface.
type String struct {
	mu sync.RWMutex
	s  string
}

func (v *String) String() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return strconv.Quote(v.s)
}

func (v *String) Set(value string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.s = value
}

func (v *String) StringValue() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.s
}
