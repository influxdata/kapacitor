package kapacitor

import (
	"math"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

type circularInt int
type circularIntPtr *int

//go:generate tmpl --o=circularqueue.gen_test.go  -data "[ \"circularInt\" , \"circularIntPtr\" ]" circularqueue.gen.go.tmpl

func Test_intCircularBufPeek(t *testing.T) {
	exp := []circularInt{1, 2, 3, 4, 5, 6, 7}
	peekRes := []circularInt{}
	q := newCircularIntCircularQueue([]circularInt{1, 2, 3, 4, 5, 6, 7}...)
	for i := 0; i < q.Len(); i++ {
		peekRes = append(peekRes, q.Peek(i))
	}
	if !reflect.DeepEqual(peekRes, exp) {
		t.Errorf("expected peeking, we would see %v, got %v", exp, peekRes)

	}
	peekRes = []circularInt{}
	for i := 0; i < q.Len(); i++ {
		peekRes = append(peekRes, q.Peek(i))

	}

	if !reflect.DeepEqual(peekRes, exp) {
		t.Errorf("expected peeking after next, we would see %v, got %v", exp, peekRes)

	}

}

func Test_intCircularBuf(t *testing.T) {
	cases := []struct {
		starting        []circularInt
		name            string
		expected        []circularInt
		expectedPrePeek [][]circularInt
		expectedPeek    [][]circularInt
		add             [][]circularInt
		dequeueTimes    []int
	}{

		{
			starting:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			name:         "remove everything but one then fill it up again",
			expected:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			expectedPeek: [][]circularInt{{6, 7}, {}, {8, 9, 10, 11}},
			add:          [][]circularInt{{}, {}, {8, 9, 10, 11}},
			dequeueTimes: []int{5, 3, 0},
		},
		{
			starting:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			name:         "regular",
			expected:     []circularInt{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expectedPeek: [][]circularInt{{11}},
			add:          [][]circularInt{{8, 9, 10, 11}},
			dequeueTimes: []int{10},
		},
		{
			starting:     nil,
			name:         "empty",
			expected:     []circularInt{},
			expectedPeek: [][]circularInt{{}},
			add:          [][]circularInt{nil},
			dequeueTimes: []int{1},
		},
		{
			starting:     nil,
			name:         "empty way past zero",
			expected:     []circularInt{},
			expectedPeek: [][]circularInt{{}, {}, {}},
			add:          [][]circularInt{nil, nil, nil},
			dequeueTimes: []int{1, 2, 4},
		},
		{
			starting:     nil,
			name:         "add to empty",
			expected:     []circularInt{},
			expectedPeek: [][]circularInt{{1}},
			add:          [][]circularInt{{1}},
			dequeueTimes: []int{0},
		},
		{
			starting:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			name:         "remove then add",
			expected:     []circularInt{1, 2, 3, 4, 5, 6},
			expectedPeek: [][]circularInt{{6, 7}, {7, 8, 9, 10}},
			add:          [][]circularInt{nil, {8, 9, 10}},
			dequeueTimes: []int{5, 1},
		},
		{
			starting:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			name:         "remove then add #2",
			expected:     []circularInt{1, 2, 3, 4, 5, 6},
			expectedPeek: [][]circularInt{{6, 7, 8, 9}, {7, 8, 9, 10, 11, 12}},
			add:          [][]circularInt{{8, 9}, {10, 11, 12}},
			dequeueTimes: []int{5, 1},
		},
		{
			starting:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			name:         "remove then add #3",
			expected:     []circularInt{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
			expectedPeek: [][]circularInt{{6, 7, 8}, {}, {13}},
			add:          [][]circularInt{{8}, {9, 10, 11}, {12, 13}},
			dequeueTimes: []int{5, 7, 1},
		},
		{
			starting:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			name:         "remove then add #4",
			expected:     []circularInt{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			expectedPeek: [][]circularInt{{6, 7, 8}, {11}, {12}},
			add:          [][]circularInt{{8}, {9, 10, 11}, {12}},
			dequeueTimes: []int{5, 5, 1},
		},
		{
			starting:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			name:         "remove too many too early then add one in",
			expected:     []circularInt{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			expectedPeek: [][]circularInt{{6, 7, 8}, {}, {12}},
			add:          [][]circularInt{{8}, {9, 10, 11}, {12}},
			dequeueTimes: []int{5, 33, 0},
		},
		{
			starting:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			name:         "remove everyone",
			expected:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			expectedPeek: [][]circularInt{{6, 7}, {}},
			add:          [][]circularInt{{}, {}},
			dequeueTimes: []int{5, 3},
		},
		{
			starting:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			name:         "remove everything but one then fill it up again",
			expected:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			expectedPeek: [][]circularInt{{6, 7}, {}, {8, 9, 10, 11}},
			add:          [][]circularInt{{}, {}, {8, 9, 10, 11}},
			dequeueTimes: []int{5, 3, 0},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := newCircularIntCircularQueue(c.starting...)
			res := []circularInt{}
			for j := 0; j < len(c.dequeueTimes); j++ {
				for i := range c.add[j] {
					q.Enqueue(c.add[j][i])
				}
				peekRes := []circularInt{}
				if len(peekRes) > 0 {
					for i := 0; i < q.Len(); i++ {
						peekRes = append(peekRes, q.Peek(i))
					}
					if !reflect.DeepEqual(peekRes, c.expectedPrePeek[j]) {
						t.Errorf("expected peeking before we called next, on step %d we would see %v, got %v", j, c.expectedPeek[j], peekRes)
					}
				}
				for i := 0; i < c.dequeueTimes[j]; i++ {
					if q.Len() > 0 {
						res = append(res, q.Peek(0))
						q.Dequeue(1)
					}
				}
				peekRes = []circularInt{}
				for i := 0; i < q.Len(); i++ {
					peekRes = append(peekRes, q.Peek(i))
				}
				if !reflect.DeepEqual(peekRes, c.expectedPeek[j]) {
					t.Errorf("expected peeking, on step %d we would see %v, got %v", j, c.expectedPeek[j], peekRes)
				}
			}
			if !reflect.DeepEqual(res, c.expected) {
				t.Errorf("expected %v, got %v", c.expected, res)
			}
		})

	}
}

func Test_leakCircularBuf(t *testing.T) {
	if testing.Short() {
		t.Skip("Testing for leaks can be slow, because of the way finalizers work")
	}
	finalizedItems := []int{} // this can't be pointers because we need the objects to leave memory
	finalizedLock := &sync.Mutex{}
	expectedFinalizedItems := []int{}
	// fill the expectedFinalizedItems
	for i := 0; i < 20; i++ {
		expectedFinalizedItems = append(expectedFinalizedItems, i)
	}

	q := newCircularIntPtrCircularQueue(nil)
	for i := 0; i < len(expectedFinalizedItems); i++ {
		i := i // make i a local object
		item := circularIntPtr(&i)
		runtime.SetFinalizer(item, func(q circularIntPtr) {
			// if the finalizer is called, that means that the GC believes the objects should be freed
			finalizedLock.Lock()
			finalizedItems = append(finalizedItems, *q)
			finalizedLock.Unlock()
		})
		q.Enqueue(item)
	}

	// go through the queue till it is empty
	q.Dequeue(q.Len())

	// the items should eventually be collected.
	// sometimes they won't be because the GC is optimizing for low latency so we try a bunch
	for i := 0; i < 100; i++ {
		finalizedLock.Lock()
		l := len(finalizedItems)
		finalizedLock.Unlock()
		if l == len(expectedFinalizedItems) {
			break
		}
		runtime.GC()
		// we have to sleep here because finalizers are async
		time.Sleep(50 * time.Millisecond)
	}
	if len(finalizedItems) != len(expectedFinalizedItems) {
		t.Errorf("expected %d objects to be freed, but got %d", len(expectedFinalizedItems), len(finalizedItems))
	}

	sort.Ints(finalizedItems)
	if !reflect.DeepEqual(finalizedItems, expectedFinalizedItems) {
		t.Errorf("The wrong items were finalized expected %v got %v", expectedFinalizedItems, finalizedItems)
	}
	// we don't want q to be freed above, when we are checking if the elements are freed,
	// so what is below is to prevent the calls to runtime.GC from freeing the whole queue early
	// the code below isn't important, just that it does something with q.data, and doesn't get
	// elided out by the compiler
	if len(q.data) == math.MaxInt32 {
		panic(q)
	}
}
