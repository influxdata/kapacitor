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

func Test_intCircularBuf(t *testing.T) {
	cases := []struct {
		starting     []circularInt
		name         string
		expected     []circularInt
		expectedPeek [][]circularInt
		add          [][]circularInt
		rem          []int
	}{
		{
			starting:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			name:         "regular",
			expected:     []circularInt{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expectedPeek: [][]circularInt{{}},
			add:          [][]circularInt{{8, 9, 10}},
			rem:          []int{10},
		},
		{
			starting:     nil,
			name:         "empty",
			expected:     []circularInt{},
			expectedPeek: [][]circularInt{{}},
			add:          [][]circularInt{nil},
			rem:          []int{1},
		},
		{
			starting:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			name:         "remove then add",
			expected:     []circularInt{1, 2, 3, 4, 5, 6},
			expectedPeek: [][]circularInt{{6, 7}, {7, 8, 9, 10}},
			add:          [][]circularInt{nil, {8, 9, 10}},
			rem:          []int{5, 1},
		},
		{
			starting:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			name:         "remove then add #2",
			expected:     []circularInt{1, 2, 3, 4, 5, 6},
			expectedPeek: [][]circularInt{{6, 7, 8}, {7, 8, 9, 10, 11}},
			add:          [][]circularInt{{8}, {9, 10, 11}},
			rem:          []int{5, 1},
		},
		{
			starting:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			name:         "remove then add #3",
			expected:     []circularInt{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
			expectedPeek: [][]circularInt{{6, 7, 8}, {}, {}},
			add:          [][]circularInt{{8}, {9, 10, 11}, {12}},
			rem:          []int{5, 6, 1},
		},
		{
			starting:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			name:         "remove then add #4",
			expected:     []circularInt{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			expectedPeek: [][]circularInt{{6, 7, 8}, {11}, {12}},
			add:          [][]circularInt{{8}, {9, 10, 11}, {12}},
			rem:          []int{5, 5, 1},
		},
		{
			starting:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			name:         "remove too many too early then add one in",
			expected:     []circularInt{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			expectedPeek: [][]circularInt{{6, 7, 8}, {}, {12}},
			add:          [][]circularInt{{8}, {9, 10, 11}, {12}},
			rem:          []int{5, 33, 0},
		},
		{
			starting:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			name:         "remove everyone",
			expected:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			expectedPeek: [][]circularInt{{6, 7}, {}},
			add:          [][]circularInt{{}, {}},
			rem:          []int{5, 2},
		},
		{
			starting:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			name:         "remove everything but one then fill it up again",
			expected:     []circularInt{1, 2, 3, 4, 5, 6, 7},
			expectedPeek: [][]circularInt{{6, 7}, {}, {8, 9, 10, 11}},
			add:          [][]circularInt{{}, {}, {8, 9, 10, 11}},
			rem:          []int{5, 2, 0},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := newCircularIntCircularQueue(c.starting...)
			res := []circularInt{}
			for j := 0; j < len(c.rem); j++ {
				for i := range c.add[j] {
					q.Enqueue(c.add[j][i])
				}
				for i := 0; i < c.rem[j] && q.Next(); i++ {
					res = append(res, q.Val())
				}
				peekRes := []circularInt{}
				p, ok := q.Peek(0)
				for i := 1; ok; i++ {
					peekRes = append(peekRes, p)
					p, ok = q.Peek(i)
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
	for q.Next() {
	}

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
