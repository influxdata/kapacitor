package tick

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLexer(t *testing.T) {
	assert := assert.New(t)

	type testCase struct {
		in     string
		tokens []token
	}

	test := func(tc testCase) {
		l := lex(tc.in)
		tokens := make([]token, 0)
		var i token
		var ok bool
		for i, ok = l.nextToken(); ok; i, ok = l.nextToken() {
			tokens = append(tokens, i)
		}
		assert.Equal(tc.tokens, tokens)
	}

	cases := []testCase{
		//Symbols
		testCase{
			in: "=",
			tokens: []token{
				token{tokenAsgn, 0, "="},
				token{tokenEOF, 1, ""},
			},
		},
		testCase{
			in: "(",
			tokens: []token{
				token{tokenLParen, 0, "("},
				token{tokenEOF, 1, ""},
			},
		},
		testCase{
			in: ")",
			tokens: []token{
				token{tokenRParen, 0, ")"},
				token{tokenEOF, 1, ""},
			},
		},
		testCase{
			in: ".",
			tokens: []token{
				token{tokenDot, 0, "."},
				token{tokenEOF, 1, ""},
			},
		},
		testCase{
			in: ";",
			tokens: []token{
				token{tokenSColon, 0, ";"},
				token{tokenEOF, 1, ""},
			},
		},
		//Numbers
		testCase{
			in: "42",
			tokens: []token{
				token{tokenNumber, 0, "42"},
				token{tokenEOF, 2, ""},
			},
		},
		testCase{
			in: "42.21",
			tokens: []token{
				token{tokenNumber, 0, "42.21"},
				token{tokenEOF, 5, ""},
			},
		},
		testCase{
			in: ".421",
			tokens: []token{
				token{tokenDot, 0, "."},
				token{tokenNumber, 1, "421"},
				token{tokenEOF, 4, ""},
			},
		},
		testCase{
			in: "0.421",
			tokens: []token{
				token{tokenNumber, 0, "0.421"},
				token{tokenEOF, 5, ""},
			},
		},
		testCase{
			in: "-42",
			tokens: []token{
				token{tokenNumber, 0, "-42"},
				token{tokenEOF, 3, ""},
			},
		},
		testCase{
			in: "-42.21",
			tokens: []token{
				token{tokenNumber, 0, "-42.21"},
				token{tokenEOF, 6, ""},
			},
		},
		testCase{
			in: "-.421",
			tokens: []token{
				token{tokenNumber, 0, "-.421"},
				token{tokenEOF, 5, ""},
			},
		},
		testCase{
			in: "-0.421",
			tokens: []token{
				token{tokenNumber, 0, "-0.421"},
				token{tokenEOF, 6, ""},
			},
		},
		//Durations
		testCase{
			in: "42s",
			tokens: []token{
				token{tokenDuration, 0, "42s"},
				token{tokenEOF, 3, ""},
			},
		},
		testCase{
			in: "42.21m",
			tokens: []token{
				token{tokenDuration, 0, "42.21m"},
				token{tokenEOF, 6, ""},
			},
		},
		testCase{
			in: ".421h",
			tokens: []token{
				token{tokenDot, 0, "."},
				token{tokenDuration, 1, "421h"},
				token{tokenEOF, 5, ""},
			},
		},
		testCase{
			in: "0.421s",
			tokens: []token{
				token{tokenDuration, 0, "0.421s"},
				token{tokenEOF, 6, ""},
			},
		},
		testCase{
			in: "1u",
			tokens: []token{
				token{tokenDuration, 0, "1u"},
				token{tokenEOF, 2, ""},
			},
		},
		testCase{
			in: "1µ",
			tokens: []token{
				token{tokenDuration, 0, "1µ"},
				token{tokenEOF, 3, ""},
			},
		},
		testCase{
			in: "1ms",
			tokens: []token{
				token{tokenDuration, 0, "1ms"},
				token{tokenEOF, 3, ""},
			},
		},
		testCase{
			in: "1h",
			tokens: []token{
				token{tokenDuration, 0, "1h"},
				token{tokenEOF, 2, ""},
			},
		},
		testCase{
			in: "1d",
			tokens: []token{
				token{tokenDuration, 0, "1d"},
				token{tokenEOF, 2, ""},
			},
		},
		testCase{
			in: "1w",
			tokens: []token{
				token{tokenDuration, 0, "1w"},
				token{tokenEOF, 2, ""},
			},
		},
		//Var
		testCase{
			in: "var ",
			tokens: []token{
				token{tokenVar, 0, "var"},
				token{tokenEOF, 4, ""},
			},
		},
		//Ident
		testCase{
			in: "variable",
			tokens: []token{
				token{tokenIdent, 0, "variable"},
				token{tokenEOF, 8, ""},
			},
		},
		testCase{
			in: "myVar01",
			tokens: []token{
				token{tokenIdent, 0, "myVar01"},
				token{tokenEOF, 7, ""},
			},
		},
		//Space
		testCase{
			in: " ",
			tokens: []token{
				token{tokenEOF, 1, ""},
			},
		},
		testCase{
			in: " \t\n",
			tokens: []token{
				token{tokenEOF, 3, ""},
			},
		},
		//Combinations
		testCase{
			in: "var x = avg();",
			tokens: []token{
				token{tokenVar, 0, "var"},
				token{tokenIdent, 4, "x"},
				token{tokenAsgn, 6, "="},
				token{tokenIdent, 8, "avg"},
				token{tokenLParen, 11, "("},
				token{tokenRParen, 12, ")"},
				token{tokenSColon, 13, ";"},
				token{tokenEOF, 14, ""},
			},
		},
		testCase{
			in: "var x = avg().parallel(4);x.groupby(\"cpu\").window().period(10s);",
			tokens: []token{
				token{tokenVar, 0, "var"},
				token{tokenIdent, 4, "x"},
				token{tokenAsgn, 6, "="},
				token{tokenIdent, 8, "avg"},
				token{tokenLParen, 11, "("},
				token{tokenRParen, 12, ")"},
				token{tokenDot, 13, "."},
				token{tokenIdent, 14, "parallel"},
				token{tokenLParen, 22, "("},
				token{tokenNumber, 23, "4"},
				token{tokenRParen, 24, ")"},
				token{tokenSColon, 25, ";"},
				token{tokenIdent, 26, "x"},
				token{tokenDot, 27, "."},
				token{tokenIdent, 28, "groupby"},
				token{tokenLParen, 35, "("},
				token{tokenString, 36, `"cpu"`},
				token{tokenRParen, 41, ")"},
				token{tokenDot, 42, "."},
				token{tokenIdent, 43, "window"},
				token{tokenLParen, 49, "("},
				token{tokenRParen, 50, ")"},
				token{tokenDot, 51, "."},
				token{tokenIdent, 52, "period"},
				token{tokenLParen, 58, "("},
				token{tokenDuration, 59, "10s"},
				token{tokenRParen, 62, ")"},
				token{tokenSColon, 63, ";"},
				token{tokenEOF, 64, ""},
			},
		},
		//Comments
		testCase{
			in: "var x = avg();\n// Comment all of this is ignored\nx.groupby(\"cpu\");",
			tokens: []token{
				token{tokenVar, 0, "var"},
				token{tokenIdent, 4, "x"},
				token{tokenAsgn, 6, "="},
				token{tokenIdent, 8, "avg"},
				token{tokenLParen, 11, "("},
				token{tokenRParen, 12, ")"},
				token{tokenSColon, 13, ";"},
				token{tokenIdent, 49, "x"},
				token{tokenDot, 50, "."},
				token{tokenIdent, 51, "groupby"},
				token{tokenLParen, 58, "("},
				token{tokenString, 59, `"cpu"`},
				token{tokenRParen, 64, ")"},
				token{tokenSColon, 65, ";"},
				token{tokenEOF, 66, ""},
			},
		},
		testCase{
			in: "var x = avg();\n// Comment all of this is ignored",
			tokens: []token{
				token{tokenVar, 0, "var"},
				token{tokenIdent, 4, "x"},
				token{tokenAsgn, 6, "="},
				token{tokenIdent, 8, "avg"},
				token{tokenLParen, 11, "("},
				token{tokenRParen, 12, ")"},
				token{tokenSColon, 13, ";"},
				token{tokenEOF, 48, ""},
			},
		},
	}

	for _, tc := range cases {
		test(tc)
	}
}
