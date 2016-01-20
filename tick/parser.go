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
	// Skip any new lines just show a single line
	if i := strings.LastIndexByte(p.Text[start:tok.pos], '\n'); i != -1 {
		start = start + i + 1
	}
	stop := tok.pos + bufSize
	if stop > len(p.Text) {
		stop = len(p.Text)
	}
	// Skip any new lines just show a single line
	if i := strings.IndexByte(p.Text[tok.pos:stop], '\n'); i != -1 {
		stop = tok.pos + i
	}
	line, char := p.lex.lineNumber(tok.pos)
	expectedStrs := make([]string, len(expected))
	for i := range expected {
		expectedStrs[i] = fmt.Sprintf("%q", expected[i])
	}
	expectedStr := strings.Join(expectedStrs, ",")
	tokStr := tok.typ.String()
	if tok.typ == TokenError {
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
	p.expect(TokenEOF)
	//if err := t.Root.Check(); err != nil {
	//	t.error(err)
	//}
}

//parse a complete program
func (p *parser) program() Node {
	l := newList(p.peek().pos)
	for {
		switch p.peek().typ {
		case TokenEOF:
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
	if p.peek().typ == TokenVar {
		n = p.declaration()
	} else {
		n = p.expression()
	}
	return n
}

//parse a declaration statement
func (p *parser) declaration() Node {
	v := p.vr()
	op := p.expect(TokenAsgn)
	b := p.expression()
	return newBinary(op, v, b)
}

//parse a 'var ident' expression
func (p *parser) vr() Node {
	p.expect(TokenVar)
	ident := p.expect(TokenIdent)
	return newIdent(ident.pos, ident.val)
}

//parse an expression
func (p *parser) expression() Node {
	switch p.peek().typ {
	case TokenIdent:
		term := p.funcOrIdent()
		return p.chain(term)
	default:
		return p.primary()
	}
}

//parse a function or identifier invocation chain
// '.' operator is left-associative
func (p *parser) chain(lhs Node) Node {
	for look := p.peek().typ; look == TokenDot; look = p.peek().typ {
		op := p.next()
		rhs := p.funcOrIdent()
		lhs = newBinary(op, lhs, rhs)
	}
	return lhs
}

func (p *parser) funcOrIdent() (n Node) {
	p.next()
	if p.peek().typ == TokenLParen {
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
	ident := p.expect(TokenIdent)
	n := newIdent(ident.pos, ident.val)
	return n
}

//parse a function call
func (p *parser) function() Node {
	ident := p.expect(TokenIdent)
	p.expect(TokenLParen)
	args := p.parameters()
	p.expect(TokenRParen)

	n := newFunc(ident.pos, ident.val, args)
	return n
}

//parse a parameter list
func (p *parser) parameters() (args []Node) {
	for {
		if p.peek().typ == TokenRParen {
			return
		}
		args = append(args, p.parameter())
		if p.next().typ != TokenComma {
			p.backup()
			return
		}
	}
}

func (p *parser) parameter() (n Node) {
	switch p.peek().typ {
	case TokenIdent:
		n = p.expression()
	case TokenLambda:
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
	TokenOr:            0,
	TokenAnd:           1,
	TokenEqual:         2,
	TokenNotEqual:      2,
	TokenRegexEqual:    2,
	TokenRegexNotEqual: 2,
	TokenGreater:       3,
	TokenGreaterEqual:  3,
	TokenLess:          3,
	TokenLessEqual:     3,
	TokenPlus:          4,
	TokenMinus:         4,
	TokenMult:          5,
	TokenDiv:           5,
	TokenMod:           5,
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
	ident := p.expect(TokenIdent)
	p.expect(TokenLParen)
	args := p.lparameters()
	p.expect(TokenRParen)

	n := newFunc(ident.pos, ident.val, args)
	return n
}

//parse a parameter list in a lfunction
func (p *parser) lparameters() (args []Node) {
	for {
		if p.peek().typ == TokenRParen {
			return
		}
		args = append(args, p.lparameter())
		if p.next().typ != TokenComma {
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
	case tok.typ == TokenLParen:
		p.next()
		n := p.lambdaExpr()
		p.expect(TokenRParen)
		return n
	case tok.typ == TokenNumber:
		return p.number()
	case tok.typ == TokenString:
		return p.string()
	case tok.typ == TokenTrue, tok.typ == TokenFalse:
		return p.boolean()
	case tok.typ == TokenDuration:
		return p.duration()
	case tok.typ == TokenRegex:
		return p.regex()
	case tok.typ == TokenMult:
		return p.star()
	case tok.typ == TokenReference:
		return p.reference()
	case tok.typ == TokenIdent:
		p.next()
		if p.peek().typ == TokenLParen {
			p.backup()
			return p.lfunction()
		}
		p.backup()
		return p.identifier()
	case tok.typ == TokenMinus, tok.typ == TokenNot:
		p.next()
		return newUnary(tok, p.primary())
	default:
		p.unexpected(
			tok,
			TokenNumber,
			TokenString,
			TokenDuration,
			TokenIdent,
			TokenTrue,
			TokenFalse,
			TokenEqual,
			TokenLParen,
			TokenMinus,
			TokenNot,
		)
		return nil
	}
}

//parse a duration literal
func (p *parser) duration() Node {
	token := p.expect(TokenDuration)
	num, err := newDur(token.pos, token.val)
	if err != nil {
		p.error(err)
	}
	return num
}

//parse a number literal
func (p *parser) number() Node {
	token := p.expect(TokenNumber)
	num, err := newNumber(token.pos, token.val)
	if err != nil {
		p.error(err)
	}
	return num
}

//parse a string literal
func (p *parser) string() Node {
	token := p.expect(TokenString)
	s := newString(token.pos, token.val)
	return s
}

//parse a regex literal
func (p *parser) regex() Node {
	token := p.expect(TokenRegex)
	r, err := newRegex(token.pos, token.val)
	if err != nil {
		p.error(err)
	}
	return r
}

// parse '*' literal
func (p *parser) star() Node {
	tok := p.expect(TokenMult)
	return newStar(tok.pos)
}

//parse a reference literal
func (p *parser) reference() Node {
	token := p.expect(TokenReference)
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
