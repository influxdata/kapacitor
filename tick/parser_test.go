package tick

import (
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParserLookAhead(t *testing.T) {
	assert := assert.New(t)

	p := &parser{}
	p.lex = lex("0 1 2 3")

	assert.Equal(token{TokenNumber, 0, "0"}, p.next())
	assert.Equal(token{TokenNumber, 2, "1"}, p.peek())
	assert.Equal(token{TokenNumber, 2, "1"}, p.next())
	p.backup()
	assert.Equal(token{TokenNumber, 2, "1"}, p.next())
	assert.Equal(token{TokenNumber, 4, "2"}, p.peek())
	p.backup()
	assert.Equal(token{TokenNumber, 2, "1"}, p.next())
	assert.Equal(token{TokenNumber, 4, "2"}, p.next())
	assert.Equal(token{TokenNumber, 6, "3"}, p.next())
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
			Error: `parser: unexpected EOF line 4 char 9 in "var b = ". expected: "number","string","duration","identifier","TRUE","FALSE","==","(","-","!"`,
		},
		testCase{
			Text:  "a\n\n\nvar b = stream.window()var period)\n\nvar x = 1",
			Error: `parser: unexpected ) line 4 char 34 in "var period)". expected: "="`,
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
			script: `var x = 'str'`,
			Root: &ListNode{
				Nodes: []Node{
					&BinaryNode{
						pos:      6,
						Operator: TokenAsgn,
						Left: &IdentifierNode{
							pos:   4,
							Ident: "x",
						},
						Right: &StringNode{
							pos:     8,
							Literal: "str",
						},
					},
				},
			},
		},
		{
			script: `var x = TRUE`,
			Root: &ListNode{
				Nodes: []Node{
					&BinaryNode{
						pos:      6,
						Operator: TokenAsgn,
						Left: &IdentifierNode{
							pos:   4,
							Ident: "x",
						},
						Right: &BoolNode{
							pos:  8,
							Bool: true,
						},
					},
				},
			},
		},
		{
			script: `var x = !FALSE`,
			Root: &ListNode{
				Nodes: []Node{
					&BinaryNode{
						pos:      6,
						Operator: TokenAsgn,
						Left: &IdentifierNode{
							pos:   4,
							Ident: "x",
						},
						Right: &UnaryNode{
							pos:      8,
							Operator: TokenNot,
							Node: &BoolNode{
								pos:  9,
								Bool: false,
							},
						},
					},
				},
			},
		},
		{
			script: `var x = 1`,
			Root: &ListNode{
				Nodes: []Node{
					&BinaryNode{
						pos:      6,
						Operator: TokenAsgn,
						Left: &IdentifierNode{
							pos:   4,
							Ident: "x",
						},
						Right: &NumberNode{
							pos:   8,
							IsInt: true,
							Int64: 1,
						},
					},
				},
			},
		},
		{
			script: `var x = -1`,
			Root: &ListNode{
				Nodes: []Node{
					&BinaryNode{
						pos:      6,
						Operator: TokenAsgn,
						Left: &IdentifierNode{
							pos:   4,
							Ident: "x",
						},
						Right: &UnaryNode{
							pos:      8,
							Operator: TokenMinus,
							Node: &NumberNode{
								pos:   9,
								IsInt: true,
								Int64: 1,
							},
						},
					},
				},
			},
		},
		{
			script: `var x = 1.0`,
			Root: &ListNode{
				Nodes: []Node{
					&BinaryNode{
						pos:      6,
						Operator: TokenAsgn,
						Left: &IdentifierNode{
							pos:   4,
							Ident: "x",
						},
						Right: &NumberNode{
							pos:     8,
							IsFloat: true,
							Float64: 1.0,
						},
					},
				},
			},
		},
		{
			script: `var x = -1.0`,
			Root: &ListNode{
				Nodes: []Node{
					&BinaryNode{
						pos:      6,
						Operator: TokenAsgn,
						Left: &IdentifierNode{
							pos:   4,
							Ident: "x",
						},
						Right: &UnaryNode{
							pos:      8,
							Operator: TokenMinus,
							Node: &NumberNode{
								pos:     9,
								IsFloat: true,
								Float64: 1.0,
							},
						},
					},
				},
			},
		},
		{
			script: `var x = 5h`,
			Root: &ListNode{
				Nodes: []Node{
					&BinaryNode{
						pos:      6,
						Operator: TokenAsgn,
						Left: &IdentifierNode{
							pos:   4,
							Ident: "x",
						},
						Right: &DurationNode{
							pos: 8,
							Dur: time.Hour * 5,
						},
					},
				},
			},
		},
		{
			script: `var x = -5h`,
			Root: &ListNode{
				Nodes: []Node{
					&BinaryNode{
						pos:      6,
						Operator: TokenAsgn,
						Left: &IdentifierNode{
							pos:   4,
							Ident: "x",
						},
						Right: &UnaryNode{
							pos:      8,
							Operator: TokenMinus,
							Node: &DurationNode{
								pos: 9,
								Dur: time.Hour * 5,
							},
						},
					},
				},
			},
		},
		{
			script: `var x = /.*\//`,
			Root: &ListNode{
				Nodes: []Node{
					&BinaryNode{
						pos:      6,
						Operator: TokenAsgn,
						Left: &IdentifierNode{
							pos:   4,
							Ident: "x",
						},
						Right: &RegexNode{
							pos:   8,
							Regex: regexp.MustCompile(".*/"),
						},
					},
				},
			},
		},
		{
			script: `var x = a.f()`,
			Root: &ListNode{
				Nodes: []Node{
					&BinaryNode{
						pos:      6,
						Operator: TokenAsgn,
						Left: &IdentifierNode{
							pos:   4,
							Ident: "x",
						},
						Right: &BinaryNode{
							pos:      9,
							Operator: TokenDot,
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
			script: `var x = 3m
			var y = -x`,
			Root: &ListNode{
				Nodes: []Node{
					&BinaryNode{
						pos:      6,
						Operator: TokenAsgn,
						Left: &IdentifierNode{
							pos:   4,
							Ident: "x",
						},
						Right: &DurationNode{
							pos: 8,
							Dur: time.Minute * 3,
						},
					},
					&BinaryNode{
						pos:      20,
						Operator: TokenAsgn,
						Left: &IdentifierNode{
							pos:   18,
							Ident: "y",
						},
						Right: &UnaryNode{
							pos:      22,
							Operator: TokenMinus,
							Node: &IdentifierNode{
								pos:   23,
								Ident: "x",
							},
						},
					},
				},
			},
		},
		{
			script: `var t = 42
			stream.where(lambda: "value" > t)
			`,
			Root: &ListNode{
				Nodes: []Node{
					&BinaryNode{
						pos:      6,
						Operator: TokenAsgn,
						Left: &IdentifierNode{
							pos:   4,
							Ident: "t",
						},
						Right: &NumberNode{
							pos:   8,
							IsInt: true,
							Int64: 42,
						},
					},
					&BinaryNode{
						pos:      20,
						Operator: TokenDot,
						Left: &IdentifierNode{
							pos:   14,
							Ident: "stream",
						},
						Right: &FunctionNode{
							pos:  21,
							Func: "where",
							Args: []Node{
								&LambdaNode{
									pos: 27,
									Node: &BinaryNode{
										pos:      43,
										Operator: TokenGreater,
										Left: &ReferenceNode{
											pos:       35,
											Reference: "value",
										},
										Right: &IdentifierNode{
											pos:   45,
											Ident: "t",
										},
									},
								},
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
					Operator: TokenAsgn,
					Left: &IdentifierNode{
						pos:   5,
						Ident: "x",
					},
					Right: &BinaryNode{
						pos:      57,
						Operator: TokenDot,
						Left: &BinaryNode{
							pos:      44,
							Operator: TokenDot,
							Left: &BinaryNode{
								pos:      30,
								Operator: TokenDot,
								Left: &BinaryNode{
									pos:      18,
									Operator: TokenDot,
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
								Operator: TokenDot,
								Left: &BinaryNode{
									pos:      70,
									Operator: TokenDot,
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
