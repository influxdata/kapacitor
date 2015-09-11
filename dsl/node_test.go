package dsl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestnumberNode(t *testing.T) {
	assert := assert.New(t)

	type testCase struct {
		Text    string
		Pos     int
		IsUint  bool
		IsFloat bool
		Uint64  uint64
		Float64 float64
		Err     error
	}

	test := func(tc testCase) {
		n, err := newNumber(tc.Pos, tc.Text)
		if tc.Err != nil {
			assert.Equal(tc.Err, err)
		} else {
			if !assert.NotNil(n) {
				t.FailNow()
			}
			assert.Equal(nodeNumber, n.nodeType)
			assert.Equal(tc.Text, n.Text)
			assert.Equal(tc.Pos, int(n.pos))
			assert.Equal(tc.IsUint, n.IsUint)
			assert.Equal(tc.IsFloat, n.IsFloat)
			assert.Equal(tc.Uint64, n.Uint64)
			assert.Equal(tc.Float64, n.Float64)
		}
	}

	cases := []testCase{
		testCase{
			Text:   "04",
			Pos:    6,
			IsUint: true,
			Uint64: 4,
		},
		testCase{
			Text:   "42",
			Pos:    5,
			IsUint: true,
			Uint64: 42,
		},
		testCase{
			Text:    "42.21",
			Pos:     4,
			IsFloat: true,
			Float64: 42.21,
		},
		testCase{
			Text:    "42.",
			Pos:     3,
			IsFloat: true,
			Float64: 42.0,
		},
		testCase{
			Text:    "0.42",
			Pos:     2,
			IsFloat: true,
			Float64: 0.42,
		},
		testCase{
			Text: "0.4.2",
			Err:  fmt.Errorf("illegal number syntax: %q", "0.4.2"),
		},
		testCase{
			Text: "0x04",
			Err:  fmt.Errorf("illegal number syntax: %q", "0x04"),
		},
	}

	for _, tc := range cases {
		test(tc)
	}
}

func TestNewbinaryNode(t *testing.T) {
	assert := assert.New(t)

	type testCase struct {
		Left     node
		Right    node
		Operator token
	}

	test := func(tc testCase) {
		n := newBinary(tc.Operator, tc.Left, tc.Right)
		if !assert.NotNil(n) {
			t.FailNow()
		}
		assert.Equal(nodeBinary, n.nodeType)
		assert.Equal(tc.Operator.pos, int(n.pos))
		assert.Equal(tc.Left, n.Left)
		assert.Equal(tc.Right, n.Right)
		assert.Equal(tc.Operator, n.Operator)
	}

	cases := []testCase{
		testCase{
			Left:  nil,
			Right: nil,
			Operator: token{
				pos: 0,
				typ: tokenAsgn,
				val: "=",
			},
		},
	}
	for _, tc := range cases {
		test(tc)
	}
}
