package kapacitor

import (
	"reflect"
	"testing"
)

func Test_Combination_Count(t *testing.T) {
	c := combination{max: 1e9}
	testCases := []struct {
		n, k, exp int64
	}{
		{
			n:   1,
			k:   0,
			exp: 1,
		},
		{
			n:   1,
			k:   1,
			exp: 1,
		},
		{
			n:   2,
			k:   1,
			exp: 2,
		},
		{
			n:   5,
			k:   2,
			exp: 10,
		},
		{
			n:   5,
			k:   3,
			exp: 10,
		},
		{
			n:   52,
			k:   5,
			exp: 2598960,
		},
	}
	for _, tc := range testCases {
		if exp, got := tc.exp, c.Count(tc.n, tc.k); exp != got {
			t.Errorf("unexpected combination count for %d choose %d: got %d exp %d", tc.n, tc.k, got, exp)
		}
	}
}
func Test_Combination_Do(t *testing.T) {
	c := combination{max: 1e9}
	testCases := []struct {
		n, k int
		exp  [][]int
	}{
		{
			n:   1,
			k:   1,
			exp: [][]int{{0}},
		},
		{
			n: 5,
			k: 2,
			exp: [][]int{
				{0, 1},
				{0, 2},
				{0, 3},
				{0, 4},
				{1, 2},
				{1, 3},
				{1, 4},
				{2, 3},
				{2, 4},
				{3, 4},
			},
		},
		{
			n: 5,
			k: 3,
			exp: [][]int{
				{0, 1, 2},
				{0, 1, 3},
				{0, 1, 4},
				{0, 2, 3},
				{0, 2, 4},
				{0, 3, 4},
				{1, 2, 3},
				{1, 2, 4},
				{1, 3, 4},
				{2, 3, 4},
			},
		},
		{
			n: 7,
			k: 5,
			exp: [][]int{
				{0, 1, 2, 3, 4},
				{0, 1, 2, 3, 5},
				{0, 1, 2, 3, 6},
				{0, 1, 2, 4, 5},
				{0, 1, 2, 4, 6},
				{0, 1, 2, 5, 6},
				{0, 1, 3, 4, 5},
				{0, 1, 3, 4, 6},
				{0, 1, 3, 5, 6},
				{0, 1, 4, 5, 6},
				{0, 2, 3, 4, 5},
				{0, 2, 3, 4, 6},
				{0, 2, 3, 5, 6},
				{0, 2, 4, 5, 6},
				{0, 3, 4, 5, 6},
				{1, 2, 3, 4, 5},
				{1, 2, 3, 4, 6},
				{1, 2, 3, 5, 6},
				{1, 2, 4, 5, 6},
				{1, 3, 4, 5, 6},
				{2, 3, 4, 5, 6},
			},
		},
	}
	for _, tc := range testCases {
		i := 0
		c.Do(tc.n, tc.k, func(indices []int) error {
			if i == len(tc.exp) {
				t.Fatalf("too many combinations returned for %d choose %d: got %v", tc.n, tc.k, indices)
			}
			if !reflect.DeepEqual(tc.exp[i], indices) {
				t.Errorf("unexpected combination set for %d choose %d index %d: got %v exp %v", tc.n, tc.k, i, indices, tc.exp[i])
			}
			i++
			return nil
		})
		if i != len(tc.exp) {
			t.Errorf("not enough combinations returned for %d choose %d", tc.n, tc.k)
		}
	}
}
