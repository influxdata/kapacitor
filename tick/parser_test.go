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
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  6,
							line: 1,
							char: 7,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Right: &StringNode{
							position: position{
								pos:  8,
								line: 1,
								char: 9,
							},
							Literal: "str",
						},
					},
				},
			},
		},
		{
			script: `var x = TRUE`,
			Root: &ListNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  6,
							line: 1,
							char: 7,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Right: &BoolNode{
							position: position{
								pos:  8,
								line: 1,
								char: 9,
							},
							Bool: true,
						},
					},
				},
			},
		},
		{
			script: `var x = !FALSE`,
			Root: &ListNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  6,
							line: 1,
							char: 7,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Right: &UnaryNode{
							position: position{
								pos:  8,
								line: 1,
								char: 9,
							},
							Operator: TokenNot,
							Node: &BoolNode{
								position: position{
									pos:  9,
									line: 1,
									char: 10,
								},
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
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  6,
							line: 1,
							char: 7,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Right: &NumberNode{
							position: position{
								pos:  8,
								line: 1,
								char: 9,
							},
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
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  6,
							line: 1,
							char: 7,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Right: &UnaryNode{
							position: position{
								pos:  8,
								line: 1,
								char: 9,
							},
							Operator: TokenMinus,
							Node: &NumberNode{
								position: position{
									pos:  9,
									line: 1,
									char: 10,
								},
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
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  6,
							line: 1,
							char: 7,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Right: &NumberNode{
							position: position{
								pos:  8,
								line: 1,
								char: 9,
							},
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
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  6,
							line: 1,
							char: 7,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Right: &UnaryNode{
							position: position{
								pos:  8,
								line: 1,
								char: 9,
							},
							Operator: TokenMinus,
							Node: &NumberNode{
								position: position{
									pos:  9,
									line: 1,
									char: 10,
								},
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
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  6,
							line: 1,
							char: 7,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Right: &DurationNode{
							position: position{
								pos:  8,
								line: 1,
								char: 9,
							},
							Dur: time.Hour * 5,
						},
					},
				},
			},
		},
		{
			script: `var x = -5h`,
			Root: &ListNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  6,
							line: 1,
							char: 7,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Right: &UnaryNode{
							position: position{
								pos:  8,
								line: 1,
								char: 9,
							},
							Operator: TokenMinus,
							Node: &DurationNode{
								position: position{
									pos:  9,
									line: 1,
									char: 10,
								},
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
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  6,
							line: 1,
							char: 7,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Right: &RegexNode{
							position: position{
								pos:  8,
								line: 1,
								char: 9,
							},
							Regex: regexp.MustCompile(".*/"),
						},
					},
				},
			},
		},
		{
			script: `var x = a.f()`,
			Root: &ListNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  6,
							line: 1,
							char: 7,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Right: &ChainNode{
							position: position{
								pos:  9,
								line: 1,
								char: 10,
							},
							Operator: TokenDot,
							Left: &IdentifierNode{
								position: position{
									pos:  8,
									line: 1,
									char: 9,
								},
								Ident: "a",
							},
							Right: &FunctionNode{
								position: position{
									pos:  10,
									line: 1,
									char: 11,
								},
								Type: propertyFunc,
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
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  6,
							line: 1,
							char: 7,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Right: &DurationNode{
							position: position{
								pos:  8,
								line: 1,
								char: 9,
							},
							Dur: time.Minute * 3,
						},
					},
					&DeclarationNode{
						position: position{
							pos:  20,
							line: 2,
							char: 10,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  18,
								line: 2,
								char: 8,
							},
							Ident: "y",
						},
						Right: &UnaryNode{
							position: position{
								pos:  22,
								line: 2,
								char: 12,
							},
							Operator: TokenMinus,
							Node: &IdentifierNode{
								position: position{
									pos:  23,
									line: 2,
									char: 13,
								},
								Ident: "x",
							},
						},
					},
				},
			},
		},
		{
			script: `var x = a|b()`,
			Root: &ListNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  6,
							line: 1,
							char: 7,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Right: &ChainNode{
							position: position{
								pos:  9,
								line: 1,
								char: 10,
							},
							Operator: TokenPipe,
							Left: &IdentifierNode{
								position: position{
									pos:  8,
									line: 1,
									char: 9,
								},
								Ident: "a",
							},
							Right: &FunctionNode{
								position: position{
									pos:  10,
									line: 1,
									char: 11,
								},
								Type: chainFunc,
								Func: "b",
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
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  6,
							line: 1,
							char: 7,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "t",
						},
						Right: &NumberNode{
							position: position{
								pos:  8,
								line: 1,
								char: 9,
							},
							IsInt: true,
							Int64: 42,
						},
					},
					&ChainNode{
						position: position{
							pos:  20,
							line: 2,
							char: 10,
						},
						Operator: TokenDot,
						Left: &IdentifierNode{
							position: position{
								pos:  14,
								line: 2,
								char: 4,
							},
							Ident: "stream",
						},
						Right: &FunctionNode{
							position: position{
								pos:  21,
								line: 2,
								char: 11,
							},
							Type: propertyFunc,
							Func: "where",
							Args: []Node{
								&LambdaNode{
									position: position{
										pos:  27,
										line: 2,
										char: 17,
									},
									Node: &BinaryNode{
										position: position{
											pos:  43,
											line: 2,
											char: 33,
										},
										Operator: TokenGreater,
										Left: &ReferenceNode{
											position: position{
												pos:  35,
												line: 2,
												char: 25,
											},
											Reference: "value",
										},
										Right: &IdentifierNode{
											position: position{
												pos:  45,
												line: 2,
												char: 35,
											},
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
			script: `global(lambda: 
// If this
// is less than that
(1 + 2 - 3 * 4 / 5) 
< (sin(6))
AND 
// more comments.
(TRUE OR FALSE))`,
			Root: &ListNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&FunctionNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
						},
						Func: "global",
						Type: globalFunc,
						Args: []Node{
							&LambdaNode{
								position: position{
									pos:  7,
									line: 1,
									char: 8,
								},
								Node: &BinaryNode{
									position: position{
										pos:  80,
										line: 6,
										char: 1,
									},
									Operator:  TokenAnd,
									MultiLine: true,
									Left: &BinaryNode{
										position: position{
											pos:  69,
											line: 5,
											char: 1,
										},
										Operator:  TokenLess,
										MultiLine: true,
										Left: &BinaryNode{
											position: position{
												pos:  51,
												line: 4,
												char: 4,
											},
											Operator: TokenPlus,
											Parens:   true,
											Left: &NumberNode{
												position: position{
													pos:  49,
													line: 4,
													char: 2,
												},
												IsInt: true,
												Int64: 1,
											},
											Right: &BinaryNode{
												position: position{
													pos:  55,
													line: 4,
													char: 8,
												},
												Operator: TokenMinus,
												Left: &NumberNode{
													position: position{
														pos:  53,
														line: 4,
														char: 6,
													},
													IsInt: true,
													Int64: 2,
												},
												Right: &BinaryNode{
													position: position{
														pos:  59,
														line: 4,
														char: 12,
													},
													Operator: TokenMult,
													Left: &NumberNode{
														position: position{
															pos:  57,
															line: 4,
															char: 10,
														},
														IsInt: true,
														Int64: 3,
													},
													Right: &BinaryNode{
														position: position{
															pos:  63,
															line: 4,
															char: 16,
														},
														Operator: TokenDiv,
														Left: &NumberNode{
															position: position{
																pos:  61,
																line: 4,
																char: 14,
															},
															IsInt: true,
															Int64: 4,
														},
														Right: &NumberNode{
															position: position{
																pos:  65,
																line: 4,
																char: 18,
															},
															IsInt: true,
															Int64: 5,
														},
													},
												},
											},
											Comment: &CommentNode{
												position: position{
													pos:  16,
													line: 2,
													char: 1,
												},
												Comments: []string{"If this", "is less than that"},
											},
										},
										Right: &FunctionNode{
											position: position{
												pos:  72,
												line: 5,
												char: 4,
											},
											Type: globalFunc,
											Func: "sin",
											Args: []Node{&NumberNode{
												position: position{
													pos:  76,
													line: 5,
													char: 8,
												},
												IsInt: true,
												Int64: 6,
											}},
										},
									},
									Right: &BinaryNode{
										position: position{
											pos:  109,
											line: 8,
											char: 7,
										},
										Operator: TokenOr,
										Parens:   true,
										Left: &BoolNode{
											position: position{
												pos:  104,
												line: 8,
												char: 2,
											},
											Bool: true,
										},
										Right: &BoolNode{
											position: position{
												pos:  112,
												line: 8,
												char: 10,
											},
											Bool: false,
										},
										Comment: &CommentNode{
											position: position{
												pos:  85,
												line: 7,
												char: 1,
											},
											Comments: []string{"more comments."},
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
			script: `// set perfect threshold
var t = 42
// only select data above threshold
stream.where(lambda: "value" > t)
			`,
			Root: &ListNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  31,
							line: 2,
							char: 7,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  29,
								line: 2,
								char: 5,
							},
							Ident: "t",
						},
						Right: &NumberNode{
							position: position{
								pos:  33,
								line: 2,
								char: 9,
							},
							IsInt: true,
							Int64: 42,
						},
						Comment: &CommentNode{
							position: position{
								pos:  0,
								line: 1,
								char: 1,
							},
							Comments: []string{"set perfect threshold"},
						},
					},
					&ChainNode{
						position: position{
							pos:  78,
							line: 4,
							char: 7,
						},
						Operator: TokenDot,
						Left: &IdentifierNode{
							position: position{
								pos:  72,
								line: 4,
								char: 1,
							},
							Ident: "stream",
							Comment: &CommentNode{
								position: position{
									pos:  36,
									line: 3,
									char: 1,
								},
								Comments: []string{"only select data above threshold"},
							},
						},
						Right: &FunctionNode{
							position: position{
								pos:  79,
								line: 4,
								char: 8,
							},
							Type: propertyFunc,
							Func: "where",
							Args: []Node{
								&LambdaNode{
									position: position{
										pos:  85,
										line: 4,
										char: 14,
									},
									Node: &BinaryNode{
										position: position{
											pos:  101,
											line: 4,
											char: 30,
										},
										Operator: TokenGreater,
										Left: &ReferenceNode{
											position: position{
												pos:  93,
												line: 4,
												char: 22,
											},
											Reference: "value",
										},
										Right: &IdentifierNode{
											position: position{
												pos:  103,
												line: 4,
												char: 32,
											},
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
		|window()
		.period(5m)
		.every(1m)
		|map(influxql.agg.mean('value'))`,
			Root: &ListNode{
				position: position{
					pos:  1,
					line: 2,
					char: 1,
				},
				Nodes: []Node{&DeclarationNode{
					position: position{
						pos:  7,
						line: 2,
						char: 7,
					},
					Left: &IdentifierNode{
						position: position{
							pos:  5,
							line: 2,
							char: 5,
						},
						Ident: "x",
					},
					Right: &ChainNode{
						position: position{
							pos:  57,
							line: 6,
							char: 3,
						},
						Operator: TokenPipe,
						Left: &ChainNode{
							position: position{
								pos:  44,
								line: 5,
								char: 3,
							},
							Operator: TokenDot,
							Left: &ChainNode{
								position: position{
									pos:  30,
									line: 4,
									char: 3,
								},
								Operator: TokenDot,
								Left: &ChainNode{
									position: position{
										pos:  18,
										line: 3,
										char: 3,
									},
									Operator: TokenPipe,
									Left: &IdentifierNode{
										position: position{
											pos:  9,
											line: 2,
											char: 9,
										},
										Ident: "stream",
									},
									Right: &FunctionNode{
										position: position{
											pos:  19,
											line: 3,
											char: 4,
										},
										Type: chainFunc,
										Func: "window",
									},
								},
								Right: &FunctionNode{
									position: position{
										pos:  31,
										line: 4,
										char: 4,
									},
									Type: propertyFunc,
									Func: "period",
									Args: []Node{&DurationNode{
										position: position{
											pos:  38,
											line: 4,
											char: 11,
										},
										Dur: 5 * time.Minute,
									}},
								},
							},
							Right: &FunctionNode{
								position: position{
									pos:  45,
									line: 5,
									char: 4,
								},
								Type: propertyFunc,
								Func: "every",
								Args: []Node{&DurationNode{
									position: position{
										pos:  51,
										line: 5,
										char: 10,
									},
									Dur: time.Minute,
								}},
							},
						},
						Right: &FunctionNode{
							position: position{
								pos:  58,
								line: 6,
								char: 4,
							},
							Type: chainFunc,
							Func: "map",
							Args: []Node{&ChainNode{
								position: position{
									pos:  74,
									line: 6,
									char: 20,
								},
								Operator: TokenDot,
								Left: &ChainNode{
									position: position{
										pos:  70,
										line: 6,
										char: 16,
									},
									Operator: TokenDot,
									Left: &IdentifierNode{
										position: position{
											pos:  62,
											line: 6,
											char: 8,
										},
										Ident: "influxql",
									},
									Right: &IdentifierNode{
										position: position{
											pos:  71,
											line: 6,
											char: 17,
										},
										Ident: "agg",
									},
								},
								Right: &FunctionNode{
									position: position{
										pos:  75,
										line: 6,
										char: 21,
									},
									Type: propertyFunc,
									Func: "mean",
									Args: []Node{&StringNode{
										position: position{
											pos:  80,
											line: 6,
											char: 26,
										},
										Literal: "value",
									}},
								},
							}},
						},
					},
				}},
			},
		},
		{
			script: `
var x = stream
		@dynamicFunc()
			.property(5m)`,
			Root: &ListNode{
				position: position{
					pos:  1,
					line: 2,
					char: 1,
				},
				Nodes: []Node{&DeclarationNode{
					position: position{
						pos:  7,
						line: 2,
						char: 7,
					},
					Left: &IdentifierNode{
						position: position{
							pos:  5,
							line: 2,
							char: 5,
						},
						Ident: "x",
					},
					Right: &ChainNode{
						position: position{
							pos:  36,
							line: 4,
							char: 4,
						},
						Operator: TokenDot,
						Left: &ChainNode{
							position: position{
								pos:  18,
								line: 3,
								char: 3,
							},
							Operator: TokenAt,
							Left: &IdentifierNode{
								position: position{
									pos:  9,
									line: 2,
									char: 9,
								},
								Ident: "stream",
							},
							Right: &FunctionNode{
								position: position{
									pos:  19,
									line: 3,
									char: 4,
								},
								Type: dynamicFunc,
								Func: "dynamicFunc",
							},
						},
						Right: &FunctionNode{
							position: position{
								pos:  37,
								line: 4,
								char: 5,
							},
							Type: propertyFunc,
							Func: "property",
							Args: []Node{&DurationNode{
								position: position{
									pos:  46,
									line: 4,
									char: 14,
								},
								Dur: time.Minute * 5,
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
			t.Fatalf("unequal trees: script:%s\ngot %v \nexp %v", tc.script, root, tc.Root)
		}
	}
}
