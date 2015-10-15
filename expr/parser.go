package expr

import (
	"fmt"
	"runtime"
)

// Tree is the representation of a parsed expression.
type Tree struct {
	Text string // text parsed to create the expression.
	Root node   // top-level root of the Tree

	// Parsing only; cleared after parse.
	lex       *lexer
	token     [2]token //two-token lookahead for parser
	peekCount int
}

// parse returns a Tree, created by parsing the expression.
// If an error is encountered, parsing stops and an empty Tree
// is returned with the error.
func Parse(text string) (t *Tree, err error) {
	t = &Tree{}
	t.Text = text
	err = t.parse(text)
	return
}

//Evaluate the Tree on a given Scope
func (t *Tree) EvalBool(v Vars, f Funcs) (bool, error) {
	if t.RType() != ReturnBool {
		return false, fmt.Errorf("Tree does not evaluate to boolean")
	}
	r, err := t.Root.ReturnBool(v, f)
	return r, err
}

//Evaluate the Tree on a given Scope
func (t *Tree) EvalNumber(v Vars, f Funcs) (float64, error) {
	if t.RType() != ReturnNum {
		return 0, fmt.Errorf("Tree does not evaluate to number")
	}
	r, err := t.Root.ReturnNum(v, f)
	return r, err
}

// Return ReturnType of tree
func (t *Tree) RType() ReturnType {
	return t.Root.RType()
}

// Parsing

// next returns the next token.
func (t *Tree) next() token {
	if t.peekCount > 0 {
		t.peekCount--
	} else {
		t.token[0], _ = t.lex.nextToken()
	}
	return t.token[t.peekCount]
}

// backup backs the input stream up one token.
func (t *Tree) backup() {
	t.peekCount++
}

// peek returns but does not consume the next token.
func (t *Tree) peek() token {
	if t.peekCount > 0 {
		return t.token[t.peekCount-1]
	}
	t.peekCount = 1
	t.token[1] = t.token[0]
	t.token[0], _ = t.lex.nextToken()
	return t.token[0]
}

// errorf formats the error and terminates processing.
func (t *Tree) errorf(format string, args ...interface{}) {
	t.Root = nil
	format = fmt.Sprintf("parser: %s", format)
	panic(fmt.Errorf(format, args...))
}

// error terminates processing.
func (t *Tree) error(err error) {
	t.errorf("%s", err)
}

// expect consumes the next token and guarantees it has the required type.
func (t *Tree) expect(expected tokenType) token {
	token := t.next()
	if token.typ != expected {
		t.unexpected(token, expected)
	}
	return token
}

// unexpected complains about the token and terminates processing.
func (t *Tree) unexpected(tok token, expected tokenType) {
	const bufSize = 10
	start := tok.pos - bufSize
	if start < 0 {
		start = 0
	}
	stop := tok.pos + bufSize
	if stop > len(t.Text) {
		stop = len(t.Text)
	}
	line, char := t.lex.lineNumber(tok.pos)
	t.errorf("unexpected %s line %d char %d in %q. expected: %q", tok, line, char, t.Text[start:stop], expected)
}

// recover is the handler that turns panics into returns from the top level of Parse.
func (t *Tree) recover(errp *error) {
	e := recover()
	if e != nil {
		if _, ok := e.(runtime.Error); ok {
			panic(e)
		}
		if t != nil {
			t.stopParse()
		}
		*errp = e.(error)
	}
	return
}

// stopParse terminates parsing.
func (t *Tree) stopParse() {
	t.lex = nil
}

// Parse parses the expression definition string to construct a representation
// of the expression for execution.
func (t *Tree) parse(text string) (err error) {
	defer t.recover(&err)
	t.lex = lex(text)
	t.Text = text
	t.Root = t.expr()
	t.expect(tokenEOF)
	if err := t.Root.Check(); err != nil {
		t.error(err)
	}
	t.stopParse()
	return nil
}

// Operator Precedence parsing
/* Psuedo Grammar
Expr -> Primary Operator Primary
Primary -> ( Expr ) | Fnc | Num | Var | String | - Primary | ! Primary
Fnc -> Var( Params )
Params -> {Primary { , Primary }
*/

var precedence = [...]int{
	tokenOr:           0,
	tokenAnd:          1,
	tokenEqual:        2,
	tokenNotEqual:     2,
	tokenGreater:      3,
	tokenGreaterEqual: 3,
	tokenLess:         3,
	tokenLessEqual:    3,
	tokenPlus:         4,
	tokenMinus:        4,
	tokenMult:         5,
	tokenDiv:          5,
}

func (t *Tree) expr() node {
	return t.prec(t.primary(), 0)

}

// parse the expression considering operator precedence.
// https://en.wikipedia.org/wiki/Operator-precedence_parser#Pseudo-code
func (t *Tree) prec(lhs node, minP int) node {
	look := t.peek()
	for isOperator(look.typ) && precedence[look.typ] >= minP {
		op := t.next()
		rhs := t.primary()
		look = t.peek()
		for isOperator(look.typ) && precedence[look.typ] >= precedence[op.typ] {
			rhs = t.prec(rhs, precedence[look.typ])
			look = t.peek()
		}
		lhs = newBinary(op, lhs, rhs)
	}
	return lhs
}

func (t *Tree) primary() node {
	switch tok := t.peek(); {
	case tok.typ == tokenLParen:
		t.next()
		n := t.expr()
		t.expect(tokenRParen)
		return n
	case tok.typ == tokenNumber:
		return t.num()
	case tok.typ == tokenIdent:
		t.next()
		if t.peek().typ == tokenLParen {
			t.backup()
			return t.fnc()
		}
		t.backup()
		return t.vr()
	case tok.typ == tokenString:
		return t.str()
	case isOperator(tok.typ):
		if tok.typ == tokenMinus {
			return newUnary(tok, t.primary())
		} else if tok.typ == tokenNot {
			return newUnary(tok, t.primary())
		} else {
			t.errorf("unexpected operator %q", tok.val)
			return nil
		}

	default:
		t.errorf("unexpected token %q", tok)
		return nil
	}
}

func (t *Tree) num() node {
	n := t.expect(tokenNumber)
	num, err := newNum(n.pos, n.val)
	if err != nil {
		t.error(err)
	}
	return num
}

func (t *Tree) vr() node {
	v := t.expect(tokenIdent)
	return newVar(v.pos, v.val)
}

func (t *Tree) str() node {
	s := t.expect(tokenString)
	return newStr(s.pos, s.val)
}

func (t *Tree) fnc() node {
	n := t.expect(tokenIdent)
	t.expect(tokenLParen)
	params := t.params()
	t.expect(tokenRParen)
	return newFunc(n.pos, n.val, params)
}

func (t *Tree) params() []node {
	params := make([]node, 0, 10)
	for {
		p := t.primary()
		params = append(params, p)
		if t.peek().typ != tokenComma {
			break
		}
	}

	return params
}
