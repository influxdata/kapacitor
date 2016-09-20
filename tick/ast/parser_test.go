package ast

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
		_, err := Parse(tc.Text)
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
			Error: `parser: unexpected ) line 4 char 34 in "var period)". expected: "identifier"`,
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
		root, err := Parse(tc.script)
		assert.Nil(err)

		//Assert we have a list of one statement
		l, ok := root.(*ProgramNode)
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
			script: `var x int`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&TypeDeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
						},
						Node: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Type: &IdentifierNode{
							position: position{
								pos:  6,
								line: 1,
								char: 7,
							},
							Ident: "int",
						},
					},
				},
			},
		},
		{
			script: `var x float`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&TypeDeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
						},
						Node: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Type: &IdentifierNode{
							position: position{
								pos:  6,
								line: 1,
								char: 7,
							},
							Ident: "float",
						},
					},
				},
			},
		},
		{
			script: `var x = 'str'`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
			script: `var x = ['str', 'asdf', 'another', s, *]`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Right: &ListNode{
							position: position{
								pos:  8,
								line: 1,
								char: 9,
							},
							Nodes: []Node{
								&StringNode{
									position: position{
										pos:  9,
										line: 1,
										char: 10,
									},
									Literal: "str",
								},
								&StringNode{
									position: position{
										pos:  16,
										line: 1,
										char: 17,
									},
									Literal: "asdf",
								},
								&StringNode{
									position: position{
										pos:  24,
										line: 1,
										char: 25,
									},
									Literal: "another",
								},
								&IdentifierNode{
									position: position{
										pos:  35,
										line: 1,
										char: 36,
									},
									Ident: "s",
								},
								&StarNode{
									position: position{
										pos:  38,
										line: 1,
										char: 39,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			script: `var x = TRUE`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
							Base:  10,
							Int64: 1,
						},
					},
				},
			},
		},
		{
			script: `var x = 0644`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
							Base:  8,
							Int64: 420,
						},
					},
				},
			},
		},
		{
			script: `var x = -0600`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
								Base:  8,
								Int64: 384,
							},
						},
					},
				},
			},
		},
		{
			script: `var x = -1`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
								Base:  10,
								Int64: 1,
							},
						},
					},
				},
			},
		},
		{
			script: `var x = 1.0`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
							Dur:     time.Hour * 5,
							Literal: "5h",
						},
					},
				},
			},
		},
		{
			script: `var x = -5h`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
								Dur:     time.Hour * 5,
								Literal: "5h",
							},
						},
					},
				},
			},
		},
		{
			script: `var x = 5 + 5`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Right: &BinaryNode{
							position: position{
								pos:  10,
								line: 1,
								char: 11,
							},
							Operator: TokenPlus,
							Left: &NumberNode{
								position: position{
									pos:  8,
									line: 1,
									char: 9,
								},
								IsInt: true,
								Base:  10,
								Int64: 5,
							},
							Right: &NumberNode{
								position: position{
									pos:  12,
									line: 1,
									char: 13,
								},
								IsInt: true,
								Base:  10,
								Int64: 5,
							},
						},
					},
				},
			},
		},
		{
			script: `f('first ' + string(5m) + 'third')`,
			Root: &ProgramNode{
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
						Type: GlobalFunc,
						Func: "f",
						Args: []Node{
							&BinaryNode{
								position: position{
									pos:  24,
									line: 1,
									char: 25,
								},
								Operator: TokenPlus,
								Left: &BinaryNode{
									position: position{
										pos:  11,
										line: 1,
										char: 12,
									},
									Operator: TokenPlus,
									Left: &StringNode{
										position: position{
											pos:  2,
											line: 1,
											char: 3,
										},
										Literal: "first ",
									},
									Right: &FunctionNode{
										position: position{
											pos:  13,
											line: 1,
											char: 14,
										},
										Type: GlobalFunc,
										Func: "string",
										Args: []Node{
											&DurationNode{
												position: position{
													pos:  20,
													line: 1,
													char: 21,
												},
												Dur:     5 * time.Minute,
												Literal: "5m",
											},
										},
									},
								},
								Right: &StringNode{
									position: position{
										pos:  26,
										line: 1,
										char: 27,
									},
									Literal: "third",
								},
							},
						},
					},
				},
			},
		},
		{
			script: `var x = 5
var y = x * 2`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
							Base:  10,
							Int64: 5,
						},
					},
					&DeclarationNode{
						position: position{
							pos:  10,
							line: 2,
							char: 1,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  14,
								line: 2,
								char: 5,
							},
							Ident: "y",
						},
						Right: &BinaryNode{
							position: position{
								pos:  20,
								line: 2,
								char: 11,
							},
							Operator: TokenMult,
							Left: &IdentifierNode{
								position: position{
									pos:  18,
									line: 2,
									char: 9,
								},
								Ident: "x",
							},
							Right: &NumberNode{
								position: position{
									pos:  22,
									line: 2,
									char: 13,
								},
								IsInt: true,
								Base:  10,
								Int64: 2,
							},
						},
					},
				},
			},
		},
		{
			script: `var x = lambda: "value" < 10`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Right: &LambdaNode{
							position: position{
								pos:  8,
								line: 1,
								char: 9,
							},
							Expression: &BinaryNode{
								position: position{
									pos:  24,
									line: 1,
									char: 25,
								},
								Operator: TokenLess,
								Left: &ReferenceNode{
									position: position{
										pos:  16,
										line: 1,
										char: 17,
									},
									Reference: "value",
								},
								Right: &NumberNode{
									position: position{
										pos:  26,
										line: 1,
										char: 27,
									},
									IsInt: true,
									Base:  10,
									Int64: 10,
								},
							},
						},
					},
				},
			},
		},
		{
			script: `var x = /.*\//`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
							Regex:   regexp.MustCompile(".*/"),
							Literal: `.*\/`,
						},
					},
				},
			},
		},
		{
			script: `var x = a.f()`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
								Type: PropertyFunc,
								Func: "f",
							},
						},
					},
				},
			},
		},
		{
			script: `var x = a.f(/.*/)`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
								Type: PropertyFunc,
								Func: "f",
								Args: []Node{
									&RegexNode{
										position: position{
											pos:  12,
											line: 1,
											char: 13,
										},
										Regex:   regexp.MustCompile(".*"),
										Literal: `.*`,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			script: `var x = int(sqrt(16.0)) + 4`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
						},
						Left: &IdentifierNode{
							position: position{
								pos:  4,
								line: 1,
								char: 5,
							},
							Ident: "x",
						},
						Right: &BinaryNode{
							position: position{
								pos:  24,
								line: 1,
								char: 25,
							},
							Operator: TokenPlus,
							Left: &FunctionNode{
								position: position{
									pos:  8,
									line: 1,
									char: 9,
								},
								Type: GlobalFunc,
								Func: "int",
								Args: []Node{
									&FunctionNode{
										position: position{
											pos:  12,
											line: 1,
											char: 13,
										},
										Type: GlobalFunc,
										Func: "sqrt",
										Args: []Node{
											&NumberNode{
												position: position{
													pos:  17,
													line: 1,
													char: 18,
												},
												IsFloat: true,
												Float64: 16.0,
											},
										},
									},
								},
							},
							Right: &NumberNode{
								position: position{
									pos:  26,
									line: 1,
									char: 27,
								},
								IsInt: true,
								Base:  10,
								Int64: 4,
							},
						},
					},
				},
			},
		},
		{
			script: `var x = a.f(y/4.0)`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
								Type: PropertyFunc,
								Func: "f",
								Args: []Node{
									&BinaryNode{
										position: position{
											pos:  13,
											line: 1,
											char: 14,
										},
										Operator: TokenDiv,
										Left: &IdentifierNode{
											position: position{
												pos:  12,
												line: 1,
												char: 13,
											},
											Ident: "y",
										},
										Right: &NumberNode{
											position: position{
												pos:  14,
												line: 1,
												char: 15,
											},
											IsFloat: true,
											Float64: 4.0,
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
			script: `var x = 3m
			var y = -x`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
							Dur:     time.Minute * 3,
							Literal: "3m",
						},
					},
					&DeclarationNode{
						position: position{
							pos:  14,
							line: 2,
							char: 4,
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
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
								Type: ChainFunc,
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
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  0,
							line: 1,
							char: 1,
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
							Base:  10,
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
							Type: PropertyFunc,
							Func: "where",
							Args: []Node{
								&LambdaNode{
									position: position{
										pos:  27,
										line: 2,
										char: 17,
									},
									Expression: &BinaryNode{
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
	// Test Left-Associative operators
	// should parse as ((1+2)-(3*4)/5)
	(1 + 2 - 3 * 4 / 5)
)
`,
			Root: &ProgramNode{
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
						Type: GlobalFunc,
						Args: []Node{
							&LambdaNode{
								position: position{
									pos:  7,
									line: 1,
									char: 8,
								},
								Expression: &BinaryNode{
									position: position{
										pos:  96,
										line: 4,
										char: 9,
									},
									Operator: TokenMinus,
									Parens:   true,
									Left: &BinaryNode{
										position: position{
											pos:  92,
											line: 4,
											char: 5,
										},
										Operator: TokenPlus,
										Left: &NumberNode{
											position: position{
												pos:  90,
												line: 4,
												char: 3,
											},
											IsInt: true,
											Base:  10,
											Int64: 1,
										},
										Right: &NumberNode{
											position: position{
												pos:  94,
												line: 4,
												char: 7,
											},
											IsInt: true,
											Base:  10,
											Int64: 2,
										},
									},
									Right: &BinaryNode{
										position: position{
											pos:  104,
											line: 4,
											char: 17,
										},
										Operator: TokenDiv,
										Left: &BinaryNode{
											position: position{
												pos:  100,
												line: 4,
												char: 13,
											},
											Operator: TokenMult,
											Left: &NumberNode{
												position: position{
													pos:  98,
													line: 4,
													char: 11,
												},
												IsInt: true,
												Base:  10,
												Int64: 3,
											},
											Right: &NumberNode{
												position: position{
													pos:  102,
													line: 4,
													char: 15,
												},
												IsInt: true,
												Base:  10,
												Int64: 4,
											},
										},
										Right: &NumberNode{
											position: position{
												pos:  106,
												line: 4,
												char: 19,
											},
											IsInt: true,
											Base:  10,
											Int64: 5,
										},
									},
									Comment: &CommentNode{
										position: position{
											pos:  17,
											line: 2,
											char: 2,
										},
										Comments: []string{"Test Left-Associative operators", "should parse as ((1+2)-(3*4)/5)"},
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
			Root: &ProgramNode{
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
						Type: GlobalFunc,
						Args: []Node{
							&LambdaNode{
								position: position{
									pos:  7,
									line: 1,
									char: 8,
								},
								Expression: &BinaryNode{
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
												pos:  55,
												line: 4,
												char: 8,
											},
											Operator: TokenMinus,
											Parens:   true,
											Left: &BinaryNode{
												position: position{
													pos:  51,
													line: 4,
													char: 4,
												},
												Operator: TokenPlus,
												Left: &NumberNode{
													position: position{
														pos:  49,
														line: 4,
														char: 2,
													},
													IsInt: true,
													Base:  10,
													Int64: 1,
												},
												Right: &NumberNode{
													position: position{
														pos:  53,
														line: 4,
														char: 6,
													},
													IsInt: true,
													Base:  10,
													Int64: 2,
												},
											},
											Right: &BinaryNode{
												position: position{
													pos:  63,
													line: 4,
													char: 16,
												},
												Operator: TokenDiv,
												Left: &BinaryNode{
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
														Base:  10,
														Int64: 3,
													},
													Right: &NumberNode{
														position: position{
															pos:  61,
															line: 4,
															char: 14,
														},
														IsInt: true,
														Base:  10,
														Int64: 4,
													},
												},
												Right: &NumberNode{
													position: position{
														pos:  65,
														line: 4,
														char: 18,
													},
													IsInt: true,
													Base:  10,
													Int64: 5,
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
											Type: GlobalFunc,
											Func: "sin",
											Args: []Node{&NumberNode{
												position: position{
													pos:  76,
													line: 5,
													char: 8,
												},
												IsInt: true,
												Base:  10,
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
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{
					&DeclarationNode{
						position: position{
							pos:  25,
							line: 2,
							char: 1,
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
							Base:  10,
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
							Type: PropertyFunc,
							Func: "where",
							Args: []Node{
								&LambdaNode{
									position: position{
										pos:  85,
										line: 4,
										char: 14,
									},
									Expression: &BinaryNode{
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
		.every(60s)
		|map(influxql.agg.mean('value'))`,
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{&DeclarationNode{
					position: position{
						pos:  1,
						line: 2,
						char: 1,
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
							pos:  58,
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
										Type: ChainFunc,
										Func: "window",
									},
								},
								Right: &FunctionNode{
									position: position{
										pos:  31,
										line: 4,
										char: 4,
									},
									Type: PropertyFunc,
									Func: "period",
									Args: []Node{&DurationNode{
										position: position{
											pos:  38,
											line: 4,
											char: 11,
										},
										Dur:     5 * time.Minute,
										Literal: "5m",
									}},
								},
							},
							Right: &FunctionNode{
								position: position{
									pos:  45,
									line: 5,
									char: 4,
								},
								Type: PropertyFunc,
								Func: "every",
								Args: []Node{&DurationNode{
									position: position{
										pos:  51,
										line: 5,
										char: 10,
									},
									Dur:     time.Minute,
									Literal: "60s",
								}},
							},
						},
						Right: &FunctionNode{
							position: position{
								pos:  59,
								line: 6,
								char: 4,
							},
							Type: ChainFunc,
							Func: "map",
							Args: []Node{&ChainNode{
								position: position{
									pos:  75,
									line: 6,
									char: 20,
								},
								Operator: TokenDot,
								Left: &ChainNode{
									position: position{
										pos:  71,
										line: 6,
										char: 16,
									},
									Operator: TokenDot,
									Left: &IdentifierNode{
										position: position{
											pos:  63,
											line: 6,
											char: 8,
										},
										Ident: "influxql",
									},
									Right: &IdentifierNode{
										position: position{
											pos:  72,
											line: 6,
											char: 17,
										},
										Ident: "agg",
									},
								},
								Right: &FunctionNode{
									position: position{
										pos:  76,
										line: 6,
										char: 21,
									},
									Type: PropertyFunc,
									Func: "mean",
									Args: []Node{&StringNode{
										position: position{
											pos:  81,
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
			Root: &ProgramNode{
				position: position{
					pos:  0,
					line: 1,
					char: 1,
				},
				Nodes: []Node{&DeclarationNode{
					position: position{
						pos:  1,
						line: 2,
						char: 1,
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
								Type: DynamicFunc,
								Func: "dynamicFunc",
							},
						},
						Right: &FunctionNode{
							position: position{
								pos:  37,
								line: 4,
								char: 5,
							},
							Type: PropertyFunc,
							Func: "property",
							Args: []Node{&DurationNode{
								position: position{
									pos:  46,
									line: 4,
									char: 14,
								},
								Dur:     time.Minute * 5,
								Literal: "5m",
							}},
						},
					},
				}},
			},
		},
	}

	for _, tc := range testCases {
		root, err := Parse(tc.script)
		if err != tc.err {
			t.Errorf("unexpected error: got %v exp %v", err, tc.err)
		} else if !reflect.DeepEqual(root, tc.Root) {
			t.Errorf("unequal trees: script:%s\ngot %v \nexp %v", tc.script, root, tc.Root)
		}
	}
}
