package tick

import (
	"fmt"
	"runtime"
	"strings"
)

// tree is the representation of a parsed dsl script.
type tree struct {
	Text string //the text being parsed
	Root Node   // top-level root of the tree

	// Parsing only; cleared after parse.
	lex       *lexer
	token     [2]token //two-token lookahead for parser
	peekCount int
}

// parse returns a Tree, created by parsing the DSL described in the
// argument string. If an error is encountered, parsing stops and an empty Tree
// is returned with the error.
func parse(text string) (Node, error) {
	t := &tree{}
	err := t.Parse(text)
	return t.Root, err
}

// --------------------
// Parsing methods
//

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
func (t *tree) unexpected(tok token, expected ...tokenType) {
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
	expectedStrs := make([]string, len(expected))
	for i := range expected {
		expectedStrs[i] = fmt.Sprintf("%q", expected[i])
	}
	expectedStr := strings.Join(expectedStrs, ",")
	tokStr := tok.typ.String()
	if tok.typ == tokenError {
		tokStr = tok.val
	}
	t.errorf("unexpected %s line %d char %d in \"%s\". expected: %s", tokStr, line, char, t.Text[start:stop], expectedStr)
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
	t.Root = t.program()
	t.expect(tokenEOF)
	//if err := t.Root.Check(); err != nil {
	//	t.error(err)
	//}
}

//parse a complete program
func (t *tree) program() Node {
	l := newList(t.peek().pos)
	for {
		switch t.peek().typ {
		case tokenEOF:
			return l
		default:
			s := t.statement()
			l.Add(s)
		}
	}
}

//parse a statement
func (t *tree) statement() Node {
	var n Node
	if t.peek().typ == tokenVar {
		n = t.declaration()
	} else {
		n = t.expression()
	}
	return n
}

//parse a declaration statement
func (t *tree) declaration() Node {
	v := t.vr()
	op := t.expect(tokenAsgn)
	b := t.expression()
	return newBinary(op, v, b)
}

//parse a 'var ident' expression
func (t *tree) vr() Node {
	t.expect(tokenVar)
	ident := t.expect(tokenIdent)
	return newIdent(ident.pos, ident.val)
}

//parse an expression
func (t *tree) expression() Node {
	term := t.funcOrIdent()
	return t.chain(term)
}

//parse a function or identifier invocation chain
// '.' operator is left-associative
func (t *tree) chain(lhs Node) Node {
	for look := t.peek().typ; look == tokenDot; look = t.peek().typ {
		op := t.next()
		rhs := t.funcOrIdent()
		lhs = newBinary(op, lhs, rhs)
	}
	return lhs
}

func (t *tree) funcOrIdent() (n Node) {
	t.next()
	if t.peek().typ == tokenLParen {
		t.backup()
		n = t.function()
	} else {
		t.backup()
		n = t.identifier()
	}
	return
}

//parse an identifier
func (t *tree) identifier() Node {
	ident := t.expect(tokenIdent)
	n := newIdent(ident.pos, ident.val)
	return n
}

//parse a function call
func (t *tree) function() Node {
	ident := t.expect(tokenIdent)
	t.expect(tokenLParen)
	args := t.parameters()
	t.expect(tokenRParen)

	n := newFunc(ident.pos, ident.val, args)
	return n
}

//parse a parameter list
func (t *tree) parameters() (args []Node) {
	for {
		if t.peek().typ == tokenRParen {
			return
		}
		args = append(args, t.parameter())
		if t.next().typ != tokenComma {
			t.backup()
			return
		}
	}
}

func (t *tree) parameter() (n Node) {
	switch t.peek().typ {
	case tokenIdent:
		n = t.expression()
	case tokenLambda:
		lambda := t.next()
		l := t.lambdaExpr()
		n = newLambda(lambda.pos, l)
	default:
		n = t.primary()
	}
	return
}

// parse the lambda expression.
func (t *tree) lambdaExpr() Node {
	return t.precedence(t.primary(), 0)
}

// Operator Precedence parsing
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

// parse the expression considering operator precedence.
// https://en.wikipedia.org/wiki/Operator-precedence_parser#Pseudo-code
func (t *tree) precedence(lhs Node, minP int) Node {
	look := t.peek()
	for isOperator(look.typ) && precedence[look.typ] >= minP {
		op := t.next()
		rhs := t.primary()
		look = t.peek()
		// right-associative
		for isOperator(look.typ) && precedence[look.typ] >= precedence[op.typ] {
			rhs = t.precedence(rhs, precedence[look.typ])
			look = t.peek()
		}
		lhs = newBinary(op, lhs, rhs)
	}
	return lhs
}

// parse a primary expression
func (t *tree) primary() Node {
	switch tok := t.peek(); {
	case tok.typ == tokenLParen:
		t.next()
		n := t.lambdaExpr()
		t.expect(tokenRParen)
		return n
	case tok.typ == tokenNumber:
		return t.number()
	case tok.typ == tokenString:
		return t.string()
	case tok.typ == tokenTrue, tok.typ == tokenFalse:
		return t.boolean()
	case tok.typ == tokenDuration:
		return t.duration()
	case tok.typ == tokenRegex:
		return t.regex()
	case tok.typ == tokenReference:
		return t.reference()
	case tok.typ == tokenIdent:
		return t.function()
	case tok.typ == tokenMinus, tok.typ == tokenNot:
		return newUnary(tok, t.primary())
	default:
		t.unexpected(
			tok,
			tokenNumber,
			tokenString,
			tokenDuration,
			tokenIdent,
			tokenTrue,
			tokenFalse,
			tokenEqual,
			tokenLParen,
			tokenMinus,
			tokenNot,
		)
		return nil
	}
}

//parse a duration literal
func (t *tree) duration() Node {
	token := t.expect(tokenDuration)
	num, err := newDur(token.pos, token.val)
	if err != nil {
		t.error(err)
	}
	return num
}

//parse a number literal
func (t *tree) number() Node {
	token := t.expect(tokenNumber)
	num, err := newNumber(token.pos, token.val)
	if err != nil {
		t.error(err)
	}
	return num
}

//parse a string literal
func (t *tree) string() Node {
	token := t.expect(tokenString)
	s := newString(token.pos, token.val)
	return s
}

//parse a regex literal
func (t *tree) regex() Node {
	token := t.expect(tokenRegex)
	r, err := newRegex(token.pos, token.val)
	if err != nil {
		t.error(err)
	}
	return r
}

//parse a reference literal
func (t *tree) reference() Node {
	token := t.expect(tokenReference)
	r := newReference(token.pos, token.val)
	return r
}

func (t *tree) boolean() Node {
	n := t.next()
	num, err := newBool(n.pos, n.val)
	if err != nil {
		t.error(err)
	}
	return num
}
