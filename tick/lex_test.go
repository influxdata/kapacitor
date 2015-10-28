package tick

import (
	"testing"
)

func TestLexer(t *testing.T) {

	type testCase struct {
		in     string
		tokens []token
	}

	test := func(tc testCase) {
		l := lex(tc.in)
		i := 0
		var tok token
		var ok bool
		for tok, ok = l.nextToken(); ok; tok, ok = l.nextToken() {
			if i >= len(tc.tokens) {
				t.Fatalf("unexpected extra token %v", tok)
			}
			if tok != tc.tokens[i] {
				t.Errorf("unexpected token: got %v exp %v i: %d in %s", tok, tc.tokens[i], i, tc.in)
			}
			i++
		}

		if i != len(tc.tokens) {
			t.Error("missing tokens", tc.tokens[i:])
		}
	}

	cases := []testCase{
		//Symbols + Operators
		{
			in: "+",
			tokens: []token{
				token{tokenPlus, 0, "+"},
				token{tokenEOF, 1, ""},
			},
		},
		{
			in: "-",
			tokens: []token{
				token{tokenMinus, 0, "-"},
				token{tokenEOF, 1, ""},
			},
		},
		{
			in: "*",
			tokens: []token{
				token{tokenMult, 0, "*"},
				token{tokenEOF, 1, ""},
			},
		},
		{
			in: "/",
			tokens: []token{
				token{tokenDiv, 0, "/"},
				token{tokenEOF, 1, ""},
			},
		},
		{
			in: "=",
			tokens: []token{
				token{tokenAsgn, 0, "="},
				token{tokenEOF, 1, ""},
			},
		},
		{
			in: "==",
			tokens: []token{
				token{tokenEqual, 0, "=="},
				token{tokenEOF, 2, ""},
			},
		},
		{
			in: "!=",
			tokens: []token{
				token{tokenNotEqual, 0, "!="},
				token{tokenEOF, 2, ""},
			},
		},
		{
			in: ">",
			tokens: []token{
				token{tokenGreater, 0, ">"},
				token{tokenEOF, 1, ""},
			},
		},
		{
			in: ">=",
			tokens: []token{
				token{tokenGreaterEqual, 0, ">="},
				token{tokenEOF, 2, ""},
			},
		},
		{
			in: "<",
			tokens: []token{
				token{tokenLess, 0, "<"},
				token{tokenEOF, 1, ""},
			},
		},
		{
			in: "<=",
			tokens: []token{
				token{tokenLessEqual, 0, "<="},
				token{tokenEOF, 2, ""},
			},
		},
		{
			in: "=~",
			tokens: []token{
				token{tokenRegexEqual, 0, "=~"},
				token{tokenEOF, 2, ""},
			},
		},
		{
			in: "!~",
			tokens: []token{
				token{tokenRegexNotEqual, 0, "!~"},
				token{tokenEOF, 2, ""},
			},
		},
		{
			in: "(",
			tokens: []token{
				token{tokenLParen, 0, "("},
				token{tokenEOF, 1, ""},
			},
		},
		{
			in: ")",
			tokens: []token{
				token{tokenRParen, 0, ")"},
				token{tokenEOF, 1, ""},
			},
		},
		{
			in: ".",
			tokens: []token{
				token{tokenDot, 0, "."},
				token{tokenEOF, 1, ""},
			},
		},
		// Keywords
		{
			in: "AND",
			tokens: []token{
				token{tokenAnd, 0, "AND"},
				token{tokenEOF, 3, ""},
			},
		},
		{
			in: "OR",
			tokens: []token{
				token{tokenOr, 0, "OR"},
				token{tokenEOF, 2, ""},
			},
		},
		{
			in: "TRUE",
			tokens: []token{
				token{tokenTrue, 0, "TRUE"},
				token{tokenEOF, 4, ""},
			},
		},
		{
			in: "FALSE",
			tokens: []token{
				token{tokenFalse, 0, "FALSE"},
				token{tokenEOF, 5, ""},
			},
		},
		{
			in: "var",
			tokens: []token{
				token{tokenVar, 0, "var"},
				token{tokenEOF, 3, ""},
			},
		},
		//Numbers
		{
			in: "42",
			tokens: []token{
				token{tokenNumber, 0, "42"},
				token{tokenEOF, 2, ""},
			},
		},
		{
			in: "42.21",
			tokens: []token{
				token{tokenNumber, 0, "42.21"},
				token{tokenEOF, 5, ""},
			},
		},
		{
			in: ".421",
			tokens: []token{
				token{tokenDot, 0, "."},
				token{tokenNumber, 1, "421"},
				token{tokenEOF, 4, ""},
			},
		},
		{
			in: "0.421",
			tokens: []token{
				token{tokenNumber, 0, "0.421"},
				token{tokenEOF, 5, ""},
			},
		},
		//Durations
		{
			in: "42s",
			tokens: []token{
				token{tokenDuration, 0, "42s"},
				token{tokenEOF, 3, ""},
			},
		},
		{
			in: "42.21m",
			tokens: []token{
				token{tokenDuration, 0, "42.21m"},
				token{tokenEOF, 6, ""},
			},
		},
		{
			in: ".421h",
			tokens: []token{
				token{tokenDot, 0, "."},
				token{tokenDuration, 1, "421h"},
				token{tokenEOF, 5, ""},
			},
		},
		{
			in: "0.421s",
			tokens: []token{
				token{tokenDuration, 0, "0.421s"},
				token{tokenEOF, 6, ""},
			},
		},
		{
			in: "1u",
			tokens: []token{
				token{tokenDuration, 0, "1u"},
				token{tokenEOF, 2, ""},
			},
		},
		{
			in: "1µ",
			tokens: []token{
				token{tokenDuration, 0, "1µ"},
				token{tokenEOF, 3, ""},
			},
		},
		{
			in: "1ms",
			tokens: []token{
				token{tokenDuration, 0, "1ms"},
				token{tokenEOF, 3, ""},
			},
		},
		{
			in: "1h",
			tokens: []token{
				token{tokenDuration, 0, "1h"},
				token{tokenEOF, 2, ""},
			},
		},
		{
			in: "1d",
			tokens: []token{
				token{tokenDuration, 0, "1d"},
				token{tokenEOF, 2, ""},
			},
		},
		{
			in: "1w",
			tokens: []token{
				token{tokenDuration, 0, "1w"},
				token{tokenEOF, 2, ""},
			},
		},
		//Identifier
		{
			in: "variable",
			tokens: []token{
				token{tokenIdent, 0, "variable"},
				token{tokenEOF, 8, ""},
			},
		},
		{
			in: "myVar01",
			tokens: []token{
				token{tokenIdent, 0, "myVar01"},
				token{tokenEOF, 7, ""},
			},
		},
		// References
		{
			in: `""`,
			tokens: []token{
				token{tokenReference, 0, `""`},
				token{tokenEOF, 2, ""},
			},
		},
		{
			in: `"ref with spaces"`,
			tokens: []token{
				token{tokenReference, 0, `"ref with spaces"`},
				token{tokenEOF, 17, ""},
			},
		},
		{
			in: `"ref\""`,
			tokens: []token{
				token{tokenReference, 0, `"ref\""`},
				token{tokenEOF, 7, ""},
			},
		},
		//Strings
		{
			in: `''`,
			tokens: []token{
				token{tokenString, 0, `''`},
				token{tokenEOF, 2, ""},
			},
		},
		{
			in: `''''''`,
			tokens: []token{
				token{tokenString, 0, `''''''`},
				token{tokenEOF, 6, ""},
			},
		},
		{
			in: `'str'`,
			tokens: []token{
				token{tokenString, 0, `'str'`},
				token{tokenEOF, 5, ""},
			},
		},
		{
			in: `'str\''`,
			tokens: []token{
				token{tokenString, 0, `'str\''`},
				token{tokenEOF, 7, ""},
			},
		},
		{
			in: `'''s'tr'''`,
			tokens: []token{
				token{tokenString, 0, `'''s'tr'''`},
				token{tokenEOF, 10, ""},
			},
		},
		{
			in: `'''s\'tr'''`,
			tokens: []token{
				token{tokenString, 0, `'''s\'tr'''`},
				token{tokenEOF, 11, ""},
			},
		},
		{
			in: `'''str'''`,
			tokens: []token{
				token{tokenString, 0, `'''str'''`},
				token{tokenEOF, 9, ""},
			},
		},
		//Space
		{
			in: " ",
			tokens: []token{
				token{tokenEOF, 1, ""},
			},
		},
		{
			in: " \t\n",
			tokens: []token{
				token{tokenEOF, 3, ""},
			},
		},
		//Combinations
		{
			in: "var x = avg()",
			tokens: []token{
				token{tokenVar, 0, "var"},
				token{tokenIdent, 4, "x"},
				token{tokenAsgn, 6, "="},
				token{tokenIdent, 8, "avg"},
				token{tokenLParen, 11, "("},
				token{tokenRParen, 12, ")"},
				token{tokenEOF, 13, ""},
			},
		},
		{
			in: "var x = avg().parallel(4)x.groupby('cpu').window().period(10s)",
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
				token{tokenIdent, 25, "x"},
				token{tokenDot, 26, "."},
				token{tokenIdent, 27, "groupby"},
				token{tokenLParen, 34, "("},
				token{tokenString, 35, "'cpu'"},
				token{tokenRParen, 40, ")"},
				token{tokenDot, 41, "."},
				token{tokenIdent, 42, "window"},
				token{tokenLParen, 48, "("},
				token{tokenRParen, 49, ")"},
				token{tokenDot, 50, "."},
				token{tokenIdent, 51, "period"},
				token{tokenLParen, 57, "("},
				token{tokenDuration, 58, "10s"},
				token{tokenRParen, 61, ")"},
				token{tokenEOF, 62, ""},
			},
		},
		//Comments
		{
			in: "var x = avg()\n// Comment all of this is ignored\nx.groupby('cpu')",
			tokens: []token{
				token{tokenVar, 0, "var"},
				token{tokenIdent, 4, "x"},
				token{tokenAsgn, 6, "="},
				token{tokenIdent, 8, "avg"},
				token{tokenLParen, 11, "("},
				token{tokenRParen, 12, ")"},
				token{tokenIdent, 48, "x"},
				token{tokenDot, 49, "."},
				token{tokenIdent, 50, "groupby"},
				token{tokenLParen, 57, "("},
				token{tokenString, 58, "'cpu'"},
				token{tokenRParen, 63, ")"},
				token{tokenEOF, 64, ""},
			},
		},
		{
			in: "var x = avg()\n// Comment all of this is ignored",
			tokens: []token{
				token{tokenVar, 0, "var"},
				token{tokenIdent, 4, "x"},
				token{tokenAsgn, 6, "="},
				token{tokenIdent, 8, "avg"},
				token{tokenLParen, 11, "("},
				token{tokenRParen, 12, ")"},
				token{tokenEOF, 47, ""},
			},
		},
	}

	for _, tc := range cases {
		test(tc)
	}
}
