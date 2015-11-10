package tick

import (
	"fmt"
	"runtime"
	"strings"
)

// tree is the representation of a parsed dsl script.
type parser struct {
	Text string //the text being parsed
	Root Node   // top-level root of the tree

	// Parsing only; cleared after parse.
	lex       *lexer
	token     [2]token //two-token lookahead for parser
	peekCount int
}

// parse returns a Node, created by parsing the DSL described in the
// argument string. If an error is encountered, parsing stops and a nil Node
// is returned with the error.
func parse(text string) (Node, error) {
	p := &parser{}
	err := p.Parse(text)
	if err != nil {
		return nil, err
	}
	return p.Root, nil
}

// --------------------
// Parsing methods
//

// next returns the next token.
func (p *parser) next() token {
	if p.peekCount > 0 {
		p.peekCount--
	} else {
		p.token[0], _ = p.lex.nextToken()
	}
	return p.token[p.peekCount]
}

// backup backs the input stream up one token.
func (p *parser) backup() {
	p.peekCount++
}

// peek returns but does not consume the next token.
func (p *parser) peek() token {
	if p.peekCount > 0 {
		return p.token[p.peekCount-1]
	}
	p.peekCount = 1
	p.token[1] = p.token[0]
	p.token[0], _ = p.lex.nextToken()
	return p.token[0]
}

// errorf formats the error and terminates processing.
func (p *parser) errorf(format string, args ...interface{}) {
	p.Root = nil
	format = fmt.Sprintf("parser: %s", format)
	panic(fmt.Errorf(format, args...))
}

// error terminates processing.
func (p *parser) error(err error) {
	p.errorf("%s", err)
}

// expect consumes the next token and guarantees it has the required type.
func (p *parser) expect(expected tokenType) token {
	token := p.next()
	if token.typ != expected {
		p.unexpected(token, expected)
	}
	return token
}

// unexpected complains about the token and terminates processing.
func (p *parser) unexpected(tok token, expected ...tokenType) {
	const bufSize = 10
	start := tok.pos - bufSize
	if start < 0 {
		start = 0
	}
	stop := tok.pos + bufSize
	if stop > len(p.Text) {
		stop = len(p.Text)
	}
	line, char := p.lex.lineNumber(tok.pos)
	expectedStrs := make([]string, len(expected))
	for i := range expected {
		expectedStrs[i] = fmt.Sprintf("%q", expected[i])
	}
	expectedStr := strings.Join(expectedStrs, ",")
	tokStr := tok.typ.String()
	if tok.typ == tokenError {
		tokStr = tok.val
	}
	p.errorf("unexpected %s line %d char %d in \"%s\". expected: %s", tokStr, line, char, p.Text[start:stop], expectedStr)
}

// recover is the handler that turns panics into returns from the top level of Parse.
func (p *parser) recover(errp *error) {
	e := recover()
	if e != nil {
		if _, ok := e.(runtime.Error); ok {
			panic(e)
		}
		if p != nil {
			p.stopParse()
		}
		*errp = e.(error)
	}
	return
}

// stopParse terminates parsing.
func (p *parser) stopParse() {
	p.lex = nil
}

// Parse parses the expression definition string to construct a representation
// of the expression for execution.
func (p *parser) Parse(text string) (err error) {
	defer p.recover(&err)
	p.lex = lex(text)
	p.Text = text
	p.parse()
	p.stopParse()
	return nil
}

// parse is the top-level parser for an expression.
// It runs to EOF.
func (p *parser) parse() {
	p.Root = p.program()
	p.expect(tokenEOF)
	//if err := t.Root.Check(); err != nil {
	//	t.error(err)
	//}
}

//parse a complete program
func (p *parser) program() Node {
	l := newList(p.peek().pos)
	for {
		switch p.peek().typ {
		case tokenEOF:
			return l
		default:
			s := p.statement()
			l.Add(s)
		}
	}
}

//parse a statement
func (p *parser) statement() Node {
	var n Node
	if p.peek().typ == tokenVar {
		n = p.declaration()
	} else {
		n = p.expression()
	}
	return n
}

//parse a declaration statement
func (p *parser) declaration() Node {
	v := p.vr()
	op := p.expect(tokenAsgn)
	b := p.expression()
	return newBinary(op, v, b)
}

//parse a 'var ident' expression
func (p *parser) vr() Node {
	p.expect(tokenVar)
	ident := p.expect(tokenIdent)
	return newIdent(ident.pos, ident.val)
}

//parse an expression
func (p *parser) expression() Node {
	term := p.funcOrIdent()
	return p.chain(term)
}

//parse a function or identifier invocation chain
// '.' operator is left-associative
func (p *parser) chain(lhs Node) Node {
	for look := p.peek().typ; look == tokenDot; look = p.peek().typ {
		op := p.next()
		rhs := p.funcOrIdent()
		lhs = newBinary(op, lhs, rhs)
	}
	return lhs
}

func (p *parser) funcOrIdent() (n Node) {
	p.next()
	if p.peek().typ == tokenLParen {
		p.backup()
		n = p.function()
	} else {
		p.backup()
		n = p.identifier()
	}
	return
}

//parse an identifier
func (p *parser) identifier() Node {
	ident := p.expect(tokenIdent)
	n := newIdent(ident.pos, ident.val)
	return n
}

//parse a function call
func (p *parser) function() Node {
	ident := p.expect(tokenIdent)
	p.expect(tokenLParen)
	args := p.parameters()
	p.expect(tokenRParen)

	n := newFunc(ident.pos, ident.val, args)
	return n
}

//parse a parameter list
func (p *parser) parameters() (args []Node) {
	for {
		if p.peek().typ == tokenRParen {
			return
		}
		args = append(args, p.parameter())
		if p.next().typ != tokenComma {
			p.backup()
			return
		}
	}
}

func (p *parser) parameter() (n Node) {
	switch p.peek().typ {
	case tokenIdent:
		n = p.expression()
	case tokenLambda:
		lambda := p.next()
		l := p.lambdaExpr()
		n = newLambda(lambda.pos, l)
	default:
		n = p.primary()
	}
	return
}

// parse the lambda expression.
func (p *parser) lambdaExpr() Node {
	return p.precedence(p.primary(), 0)
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
	tokenMod:          5,
}

// parse the expression considering operator precedence.
// https://en.wikipedia.org/wiki/Operator-precedence_parser#Pseudo-code
func (p *parser) precedence(lhs Node, minP int) Node {
	look := p.peek()
	for isOperator(look.typ) && precedence[look.typ] >= minP {
		op := p.next()
		rhs := p.primary()
		look = p.peek()
		// right-associative
		for isOperator(look.typ) && precedence[look.typ] >= precedence[op.typ] {
			rhs = p.precedence(rhs, precedence[look.typ])
			look = p.peek()
		}
		lhs = newBinary(op, lhs, rhs)
	}
	return lhs
}

//parse a function call in a lambda expr
func (p *parser) lfunction() Node {
	ident := p.expect(tokenIdent)
	p.expect(tokenLParen)
	args := p.lparameters()
	p.expect(tokenRParen)

	n := newFunc(ident.pos, ident.val, args)
	return n
}

//parse a parameter list in a lfunction
func (p *parser) lparameters() (args []Node) {
	for {
		if p.peek().typ == tokenRParen {
			return
		}
		args = append(args, p.lparameter())
		if p.next().typ != tokenComma {
			p.backup()
			return
		}
	}
}

func (p *parser) lparameter() (n Node) {
	n = p.primary()
	if isOperator(p.peek().typ) {
		n = p.precedence(n, 0)
	}
	return
}

// parse a primary expression
func (p *parser) primary() Node {
	switch tok := p.peek(); {
	case tok.typ == tokenLParen:
		p.next()
		n := p.lambdaExpr()
		p.expect(tokenRParen)
		return n
	case tok.typ == tokenNumber:
		return p.number()
	case tok.typ == tokenString:
		return p.string()
	case tok.typ == tokenTrue, tok.typ == tokenFalse:
		return p.boolean()
	case tok.typ == tokenDuration:
		return p.duration()
	case tok.typ == tokenRegex:
		return p.regex()
	case tok.typ == tokenMult:
		return p.star()
	case tok.typ == tokenReference:
		return p.reference()
	case tok.typ == tokenIdent:
		return p.lfunction()
	case tok.typ == tokenMinus, tok.typ == tokenNot:
		return newUnary(tok, p.primary())
	default:
		p.unexpected(
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
func (p *parser) duration() Node {
	token := p.expect(tokenDuration)
	num, err := newDur(token.pos, token.val)
	if err != nil {
		p.error(err)
	}
	return num
}

//parse a number literal
func (p *parser) number() Node {
	token := p.expect(tokenNumber)
	num, err := newNumber(token.pos, token.val)
	if err != nil {
		p.error(err)
	}
	return num
}

//parse a string literal
func (p *parser) string() Node {
	token := p.expect(tokenString)
	s := newString(token.pos, token.val)
	return s
}

//parse a regex literal
func (p *parser) regex() Node {
	token := p.expect(tokenRegex)
	r, err := newRegex(token.pos, token.val)
	if err != nil {
		p.error(err)
	}
	return r
}

// parse '*' literal
func (p *parser) star() Node {
	tok := p.expect(tokenMult)
	return newStar(tok.pos)
}

//parse a reference literal
func (p *parser) reference() Node {
	token := p.expect(tokenReference)
	r := newReference(token.pos, token.val)
	return r
}

func (p *parser) boolean() Node {
	n := p.next()
	num, err := newBool(n.pos, n.val)
	if err != nil {
		p.error(err)
	}
	return num
}
