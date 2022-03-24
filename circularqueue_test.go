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

func Test_intCircularBufPeek(t *testing.T) {
	exp := []int{1, 2, 3, 4, 5, 6, 7}
	peekRes := []int{}
	q := NewCircularQueue([]int{1, 2, 3, 4, 5, 6, 7}...)
	for i := 0; i < q.Len; i++ {
		peekRes = append(peekRes, Peek(q, i))
	}
	if !reflect.DeepEqual(peekRes, exp) {
		t.Errorf("expected peeking, we would see %v, got %v", exp, peekRes)

	}
	peekRes = []int{}
	for i := 0; i < q.Len; i++ {
		peekRes = append(peekRes, Peek(q, i))
	}

	if !reflect.DeepEqual(peekRes, exp) {
		t.Errorf("expected peeking after next, we would see %v, got %v", exp, peekRes)
	}

}

func Test_intCircularBuf(t *testing.T) {
	cases := []struct {
		starting        []int
		name            string
		expected        []int
		expectedPrePeek [][]int
		expectedPeek    [][]int
		add             [][]int
		dequeueTimes    []int
	}{

		{
			starting:     []int{1, 2, 3, 4, 5, 6, 7},
			name:         "remove everything but one then fill it up again",
			expected:     []int{1, 2, 3, 4, 5, 6, 7},
			expectedPeek: [][]int{{6, 7}, {}, {8, 9, 10, 11}},
			add:          [][]int{{}, {}, {8, 9, 10, 11}},
			dequeueTimes: []int{5, 3, 0},
		},
		{
			starting:     []int{1, 2, 3, 4, 5, 6, 7},
			name:         "regular",
			expected:     []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expectedPeek: [][]int{{11}},
			add:          [][]int{{8, 9, 10, 11}},
			dequeueTimes: []int{10},
		},
		{
			starting:     nil,
			name:         "empty",
			expected:     []int{},
			expectedPeek: [][]int{{}},
			add:          [][]int{nil},
			dequeueTimes: []int{1},
		},
		{
			starting:     nil,
			name:         "empty way past zero",
			expected:     []int{},
			expectedPeek: [][]int{{}, {}, {}},
			add:          [][]int{nil, nil, nil},
			dequeueTimes: []int{1, 2, 4},
		},
		{
			starting:     nil,
			name:         "add to empty",
			expected:     []int{},
			expectedPeek: [][]int{{1}},
			add:          [][]int{{1}},
			dequeueTimes: []int{0},
		},
		{
			starting:     []int{1, 2, 3, 4, 5, 6, 7},
			name:         "remove then add",
			expected:     []int{1, 2, 3, 4, 5, 6},
			expectedPeek: [][]int{{6, 7}, {7, 8, 9, 10}},
			add:          [][]int{nil, {8, 9, 10}},
			dequeueTimes: []int{5, 1},
		},
		{
			starting:     []int{1, 2, 3, 4, 5, 6, 7},
			name:         "remove then add #2",
			expected:     []int{1, 2, 3, 4, 5, 6},
			expectedPeek: [][]int{{6, 7, 8, 9}, {7, 8, 9, 10, 11, 12}},
			add:          [][]int{{8, 9}, {10, 11, 12}},
			dequeueTimes: []int{5, 1},
		},
		{
			starting:     []int{1, 2, 3, 4, 5, 6, 7},
			name:         "remove then add #3",
			expected:     []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
			expectedPeek: [][]int{{6, 7, 8}, {}, {13}},
			add:          [][]int{{8}, {9, 10, 11}, {12, 13}},
			dequeueTimes: []int{5, 7, 1},
		},
		{
			starting:     []int{1, 2, 3, 4, 5, 6, 7},
			name:         "remove then add #4",
			expected:     []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			expectedPeek: [][]int{{6, 7, 8}, {11}, {12}},
			add:          [][]int{{8}, {9, 10, 11}, {12}},
			dequeueTimes: []int{5, 5, 1},
		},
		{
			starting:     []int{1, 2, 3, 4, 5, 6, 7},
			name:         "remove too many too early then add one in",
			expected:     []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			expectedPeek: [][]int{{6, 7, 8}, {}, {12}},
			add:          [][]int{{8}, {9, 10, 11}, {12}},
			dequeueTimes: []int{5, 33, 0},
		},
		{
			starting:     []int{1, 2, 3, 4, 5, 6, 7},
			name:         "remove everyone",
			expected:     []int{1, 2, 3, 4, 5, 6, 7},
			expectedPeek: [][]int{{6, 7}, {}},
			add:          [][]int{{}, {}},
			dequeueTimes: []int{5, 3},
		},
		{
			starting:     []int{1, 2, 3, 4, 5, 6, 7},
			name:         "remove everything but one then fill it up again",
			expected:     []int{1, 2, 3, 4, 5, 6, 7},
			expectedPeek: [][]int{{6, 7}, {}, {8, 9, 10, 11}},
			add:          [][]int{{}, {}, {8, 9, 10, 11}},
			dequeueTimes: []int{5, 3, 0},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := NewCircularQueue(c.starting...)
			res := []int{}
			for j := 0; j < len(c.dequeueTimes); j++ {
				for i := range c.add[j] {
					Enqueue(q, c.add[j][i])
				}
				peekRes := []int{}
				if len(peekRes) > 0 {
					for i := 0; i < q.Len; i++ {
						peekRes = append(peekRes, Peek(q, i))
					}
					if !reflect.DeepEqual(peekRes, c.expectedPrePeek[j]) {
						t.Errorf("expected peeking before we called next, on step %d we would see %v, got %v", j, c.expectedPeek[j], peekRes)
					}
				}
				for i := 0; i < c.dequeueTimes[j]; i++ {
					if q.Len > 0 {
						res = append(res, Peek(q, 0))
						q.Dequeue(1)
					}
				}
				peekRes = []int{}
				for i := 0; i < q.Len; i++ {
					peekRes = append(peekRes, Peek(q, i))
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
	var finalizedItems []int // this can't be pointers because we need the objects to leave memory
	finalizedLock := &sync.Mutex{}
	var expectedFinalizedItems []int
	// fill the expectedFinalizedItems
	for i := 0; i < 20; i++ {
		expectedFinalizedItems = append(expectedFinalizedItems, i)
	}

	q := NewCircularQueue[*int](nil)
	for i := 0; i < len(expectedFinalizedItems); i++ {
		i := i // make i a local object
		item := &i
		runtime.SetFinalizer(item, func(q *int) {
			// if the finalizer is called, that means that the GC believes the objects should be freed
			finalizedLock.Lock()
			finalizedItems = append(finalizedItems, *q)
			finalizedLock.Unlock()
		})
		Enqueue(q, item)
	}

	// go through the queue till it is empty
	q.Dequeue(q.Len)

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
