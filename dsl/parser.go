package dsl

import (
	"fmt"
	"runtime"
)

// tree is the representation of a parsed dsl script.
type tree struct {
	Text string // text parsed to create the expression.
	Root node   // top-level root of the tree

	// Parsing only; cleared after parse.
	lex       *lexer
	token     [2]token //two-token lookahead for parser
	peekCount int
}

// parse returns a Tree, created by parsing the DSL described in the
// argument string. If an error is encountered, parsing stops and an empty Tree
// is returned with the error.
func parse(text string) (t *tree, err error) {
	t = &tree{}
	t.Text = text
	err = t.Parse(text)
	return
}

//Evaluate the tree on a given Scope
func (t *tree) Eval(s *Scope) error {
	_, err := t.Root.Return(s)
	return err
}

// Parsing

// next returns the next token.
func (t *tree) next() token {
	if t.peekCount > 0 {
		t.peekCount--
	} else {
		t.token[0], _ = t.lex.nextToken()
	}
	return t.token[t.peekCount]
}

// backup backs the input stream up one token.
func (t *tree) backup() {
	t.peekCount++
}

// peek returns but does not consume the next token.
func (t *tree) peek() token {
	if t.peekCount > 0 {
		return t.token[t.peekCount-1]
	}
	t.peekCount = 1
	t.token[1] = t.token[0]
	t.token[0], _ = t.lex.nextToken()
	return t.token[0]
}

// errorf formats the error and terminates processing.
func (t *tree) errorf(format string, args ...interface{}) {
	t.Root = nil
	format = fmt.Sprintf("parser: %s", format)
	panic(fmt.Errorf(format, args...))
}

// error terminates processing.
func (t *tree) error(err error) {
	t.errorf("%s", err)
}

// expect consumes the next token and guarantees it has the required type.
func (t *tree) expect(expected tokenType) token {
	token := t.next()
	if token.typ != expected {
		t.unexpected(token, expected)
	}
	return token
}

// unexpected complains about the token and terminates processing.
func (t *tree) unexpected(tok token, expected tokenType) {
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
func (t *tree) recover(errp *error) {
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
func (t *tree) stopParse() {
	t.lex = nil
}

// Parse parses the expression definition string to construct a representation
// of the expression for execution.
func (t *tree) Parse(text string) (err error) {
	defer t.recover(&err)
	t.lex = lex(text)
	t.Text = text
	t.parse()
	t.stopParse()
	return nil
}

// parse is the top-level parser for an expression.
// It runs to EOF.
func (t *tree) parse() {
	t.Root = t.prog()
	t.expect(tokenEOF)
	if err := t.Root.Check(); err != nil {
		t.error(err)
	}
}

/* Grammar

Prog -> Stmt { Stmt ...}
Stmt -> Decl ';' | Expr ';'
Decl -> Vr '=' Expr
Expr -> Ident { FChain }
FChain -> '.' Fnc { FChain }
Vr -> 'var' Ident
Fnc -> Ident '( Args ')'
Args -> {Param {',' Param}}
Param -> Ref | Dur | Num | Str
Ref -> Ident { RChain }
RChain -> '.' Ident { RChain }
Ident -> [a-zA-Z][a-zA-Z0-9]*
Dur -> duration
Num -> number
Str => string_literal
*/

//parse a complete program
func (t *tree) prog() node {
	l := newList(t.peek().pos)
	for {
		switch t.peek().typ {
		case tokenEOF:
			return l
		default:
			s := t.stmt()
			l.Add(s)
		}
	}
}

//parse a statement
func (t *tree) stmt() node {
	var n node
	if t.peek().typ == tokenVar {
		n = t.decl()
	} else {
		n = t.expr()
	}
	t.expect(tokenSColon)
	return n
}

//parse a declaration statement
func (t *tree) decl() node {
	v := t.vr()
	op := t.expect(tokenAsgn)
	b := t.expr()
	return newBinary(op, v, b)
}

//parse a 'var' epression
func (t *tree) vr() node {
	t.expect(tokenVar)
	ident := t.expect(tokenIdent)
	return newIdent(ident.pos, ident.val)
}

//parse an expression
func (t *tree) expr() node {
	//We reverse the tree under expr
	term := t.ident()
	if i := t.peek(); i.typ == tokenDot {
		top, bottom := t.fchain()
		top.Right = term
		return bottom
	}
	return term
}

//parse an function invocation chain
func (t *tree) fchain() (*binaryNode, *binaryNode) {
	op := t.next()
	n := t.fnc()
	b := newBinary(op, n, nil)
	if i := t.peek(); i.typ == tokenDot {
		o, bottom := t.fchain()
		o.Right = b
		return b, bottom
	}
	return b, b
}

//parse an identifier
func (t *tree) ident() node {
	ident := t.expect(tokenIdent)
	n := newIdent(ident.pos, ident.val)
	return n
}

//parse a reference
func (t *tree) ref() node {
	//We reverse the tree under ref
	term := t.ident()
	if i := t.peek(); i.typ == tokenDot {
		top, bottom := t.rchain()
		top.Right = term
		return bottom
	}
	return term
}

//parse a refernce chain
func (t *tree) rchain() (*binaryNode, *binaryNode) {
	op := t.next()
	n := t.ident()
	b := newBinary(op, n, nil)
	if i := t.peek(); i.typ == tokenDot {
		o, bottom := t.rchain()
		o.Right = b
		return b, bottom
	}
	return b, b
}

//parse a function call
func (t *tree) fnc() node {
	ident := t.expect(tokenIdent)
	t.expect(tokenLParen)
	args := t.args()
	t.expect(tokenRParen)

	n := newFunc(ident.pos, ident.val, args)
	return n
}

//parse an argument list
func (t *tree) args() []node {
	args := make([]node, 0)
ARGS:
	for {
		typ := t.peek().typ
		switch typ {
		case tokenIdent:
			args = append(args, t.ref())
		case tokenDuration:
			args = append(args, t.dur())
		case tokenNumber:
			args = append(args, t.num())
		case tokenString:
			args = append(args, t.str())
		case tokenComma:
			t.next()
		default:
			break ARGS
		}
	}

	return args
}

//parse a duration literal
func (t *tree) dur() node {
	token := t.expect(tokenDuration)
	num, err := newDur(token.pos, token.val)
	if err != nil {
		t.error(err)
	}
	return num
}

//parse a number literal
func (t *tree) num() node {
	token := t.expect(tokenNumber)
	num, err := newNumber(token.pos, token.val)
	if err != nil {
		t.error(err)
	}
	return num
}

//parse a string literal
func (t *tree) str() node {
	token := t.expect(tokenString)
	s := newString(token.pos, token.val)
	return s
}
