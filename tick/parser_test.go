package tick

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParserLookAhead(t *testing.T) {
	assert := assert.New(t)

	p := &parser{}
	p.lex = lex("0 1 2 3")

	assert.Equal(token{tokenNumber, 0, "0"}, p.next())
	assert.Equal(token{tokenNumber, 2, "1"}, p.peek())
	assert.Equal(token{tokenNumber, 2, "1"}, p.next())
	p.backup()
	assert.Equal(token{tokenNumber, 2, "1"}, p.next())
	assert.Equal(token{tokenNumber, 4, "2"}, p.peek())
	p.backup()
	assert.Equal(token{tokenNumber, 2, "1"}, p.next())
	assert.Equal(token{tokenNumber, 4, "2"}, p.next())
	assert.Equal(token{tokenNumber, 6, "3"}, p.next())
}

func TestParseErrors(t *testing.T) {
	assert := assert.New(t)

	type testCase struct {
		Text  string
		Error string
	}

	test := func(tc testCase) {
		_, err := parse(tc.Text)
		if assert.NotNil(err) {
			if e, g := tc.Error, err.Error(); g != e {
				t.Errorf("unexpected error: \ngot %s \nexp %s", g, e)
			}
		}
	}

	cases := []testCase{
		testCase{
			Text:  "a\n\n\nvar b = ",
			Error: "parser: unexpected EOF line 4 char 9 in \"\n\nvar b = \". expected: \"identifier\"",
		},
		testCase{
			Text:  "a\n\n\nvar b = stream.window()var period",
			Error: `parser: unexpected EOF line 4 char 34 in "var period". expected: "="`,
		},
		testCase{
			Text:  "a\n\n\nvar b = stream.window(\nb.period(10s)",
			Error: `parser: unexpected EOF line 5 char 14 in "eriod(10s)". expected: ")"`,
		},
	}

	for _, tc := range cases {
		test(tc)
	}
}
func TestParseStrings(t *testing.T) {
	assert := assert.New(t)

	testCases := []struct {
		script  string
		literal string
	}{
		{
			script:  `f('asdf')`,
			literal: "asdf",
		},
		{
			script:  `f('''asdf''')`,
			literal: "asdf",
		},
		{
			script:  `f('a\'sdf')`,
			literal: "a'sdf",
		},
		{
			script:  `f('''\'asdf''')`,
			literal: `\'asdf`,
		},
	}

	for _, tc := range testCases {
		root, err := parse(tc.script)
		assert.Nil(err)

		//Assert we have a list of one statement
		l, ok := root.(*ListNode)
		if !assert.True(ok, "tree.Root is not a list node") {
			t.FailNow()
		}
		if !assert.Equal(1, len(l.Nodes), "Did not get exactly one node in statement list") {
			t.FailNow()
		}

		//Assert the first statement is a function with one stringNode argument
		f, ok := l.Nodes[0].(*FunctionNode)
		if !assert.True(ok, "first statement is not a func node %q", l.Nodes[0]) {
			t.FailNow()
		}
		if !assert.Equal(1, len(f.Args), "unexpected number of args got %d exp %d", len(f.Args), 1) {
			t.FailNow()
		}
		str, ok := f.Args[0].(*StringNode)
		if !assert.True(ok, "first argument is not a string node %q", f.Args[0]) {
			t.FailNow()
		}

		// Assert strings literals are equal
		assert.Equal(tc.literal, str.Literal)
	}

}

func TestParseStatements(t *testing.T) {

	testCases := []struct {
		script string
		Root   Node
		err    error
	}{
		{
			script: `var x = a.f()`,
			Root: &ListNode{
				Nodes: []Node{
					&BinaryNode{
						pos:      6,
						Operator: tokenAsgn,
						Left: &IdentifierNode{
							pos:   4,
							Ident: "x",
						},
						Right: &BinaryNode{
							pos:      9,
							Operator: tokenDot,
							Left: &IdentifierNode{
								pos:   8,
								Ident: "a",
							},
							Right: &FunctionNode{
								pos:  10,
								Func: "f",
							},
						},
					},
				},
			},
		},
		{
			script: `
var x = stream
		.window()
		.period(5m)
		.every(1m)
		.map(influxql.agg.mean('value'))`,
			Root: &ListNode{
				pos: 1,
				Nodes: []Node{&BinaryNode{
					pos:      7,
					Operator: tokenAsgn,
					Left: &IdentifierNode{
						pos:   5,
						Ident: "x",
					},
					Right: &BinaryNode{
						pos:      57,
						Operator: tokenDot,
						Left: &BinaryNode{
							pos:      44,
							Operator: tokenDot,
							Left: &BinaryNode{
								pos:      30,
								Operator: tokenDot,
								Left: &BinaryNode{
									pos:      18,
									Operator: tokenDot,
									Left: &IdentifierNode{
										pos:   9,
										Ident: "stream",
									},
									Right: &FunctionNode{
										pos:  19,
										Func: "window",
									},
								},
								Right: &FunctionNode{
									pos:  31,
									Func: "period",
									Args: []Node{&DurationNode{
										pos: 38,
										Dur: 5 * time.Minute,
									}},
								},
							},
							Right: &FunctionNode{
								pos:  45,
								Func: "every",
								Args: []Node{&DurationNode{
									pos: 51,
									Dur: time.Minute,
								}},
							},
						},
						Right: &FunctionNode{
							pos:  58,
							Func: "map",
							Args: []Node{&BinaryNode{
								pos:      74,
								Operator: tokenDot,
								Left: &BinaryNode{
									pos:      70,
									Operator: tokenDot,
									Left: &IdentifierNode{
										pos:   62,
										Ident: "influxql",
									},
									Right: &IdentifierNode{
										pos:   71,
										Ident: "agg",
									},
								},
								Right: &FunctionNode{
									pos:  75,
									Func: "mean",
									Args: []Node{&StringNode{
										pos:     80,
										Literal: "value",
									}},
								},
							}},
						},
					},
				}},
			},
		},
	}

	for _, tc := range testCases {
		root, err := parse(tc.script)
		if err != tc.err {
			t.Fatalf("unexpected error: got %v exp %v", err, tc.err)
		}

		if !reflect.DeepEqual(root, tc.Root) {
			t.Fatalf("unequal trees: \ngot %v \nexp %v", root, tc.Root)
		}
	}
}
