package ast

import (
	"fmt"
	"runtime"
	"strings"
)

// tree is the representation of a parsed dsl script.
type parser struct {
	// the text being parsed
	text string

	lex *lexer
	// two-token lookahead for parser
	token     [2]token
	peekCount int

	// Current comment node.
	// Comments are parsed transparently via the normal next peek operations.
	comments [2]*CommentNode
}

// Parse returns a Node, created by parsing the DSL described in the
// argument string. If an error is encountered, parsing stops and a nil Node
// is returned with the error.
func Parse(text string) (Node, error) {
	p := &parser{}
	n, err := p.parse(text)
	if err != nil {
		return nil, err
	}
	return n, nil
}

// Parse returns a LambdaNode, created by parsing a lambda expression.
func ParseLambda(text string) (*LambdaNode, error) {
	p := &parser{}
	l, err := p.parseLambda(text)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (p *parser) hasNewLine(start, end int) bool {
	return strings.IndexRune(p.text[start:end], '\n') != -1
}

// --------------------
// Parsing methods
//

func (p *parser) nextToken() (t token, comment *CommentNode) {
	// When reading next token create comment node if comment.
	t, _ = p.lex.nextToken()
	var comments []string
	pos := -1
	for ; t.typ == TokenComment; t, _ = p.lex.nextToken() {
		if pos == -1 {
			pos = t.pos
		}
		comments = append(comments, t.val)
	}
	if len(comments) > 0 {
		comment = newComment(p.position(pos), comments)
	}
	return t, comment
}

// next returns the next token.
func (p *parser) next() token {
	if p.peekCount > 0 {
		p.peekCount--
	} else {
		p.token[0], p.comments[0] = p.nextToken()
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
	p.token[1], p.comments[1] = p.token[0], p.comments[0]
	p.token[0], p.comments[0] = p.nextToken()
	return p.token[0]
}

// Consume the current comment node
func (p *parser) consumeComment() *CommentNode {
	c := p.comments[p.peekCount]
	p.comments[p.peekCount] = nil
	return c
}

// errorf formats the error and terminates processing.
func (p *parser) errorf(format string, args ...interface{}) {
	format = fmt.Sprintf("parser: %s", format)
	panic(fmt.Errorf(format, args...))
}

// error terminates processing.
func (p *parser) error(err error) {
	p.errorf("%s", err)
}

// expect consumes the next token and guarantees it has the required type.
func (p *parser) expect(expected TokenType) token {
	token := p.next()
	if token.typ != expected {
		p.unexpected(token, expected)
	}
	return token
}

// unexpected complains about the token and terminates processing.
func (p *parser) unexpected(tok token, expected ...TokenType) {
	const bufSize = 10
	start := tok.pos - bufSize
	if start < 0 {
		start = 0
	}
	// Skip any new lines just show a single line
	if i := strings.LastIndexByte(p.text[start:tok.pos], '\n'); i != -1 {
		start = start + i + 1
	}
	stop := tok.pos + bufSize
	if stop > len(p.text) {
		stop = len(p.text)
	}
	// Skip any new lines just show a single line
	if i := strings.IndexByte(p.text[tok.pos:stop], '\n'); i != -1 {
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
	p.errorf("unexpected %s line %d char %d in \"%s\". expected: %s", tokStr, line, char, p.text[start:stop], expectedStr)
}

func (p *parser) position(pos int) position {
	line, char := p.lex.lineNumber(pos)
	return position{
		pos:  pos,
		line: line,
		char: char,
	}
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
func (p *parser) parse(text string) (n Node, err error) {
	defer p.recover(&err)
	p.lex = lex(text)
	p.text = text

	// Parse complete program
	n = p.program()
	p.expect(TokenEOF)

	p.stopParse()
	return
}

// Parse parses only a lambda expression starting after the lambda: keyword
func (p *parser) parseLambda(text string) (n *LambdaNode, err error) {
	defer p.recover(&err)
	p.lex = lex(text)
	p.text = text

	l := p.primaryExpr()
	n = newLambda(p.position(0), l, nil)
	p.expect(TokenEOF)

	p.stopParse()
	return
}

//parse a complete program
func (p *parser) program() Node {
	l := newProgram(p.position(0))
	var s Node
	for {
		switch p.peek().typ {
		case TokenEOF:
			if c := p.comments[1]; c != nil {
				l.Add(c)
			}
			if c := p.comments[0]; c != nil {
				l.Add(c)
			}
			return l
		default:
			s = p.statement()
			l.Add(s)
		}
	}
}

//parse a statement
func (p *parser) statement() Node {
	switch t := p.peek().typ; t {
	case TokenVar:
		return p.declaration()
	default:
		return p.expression()
	}
}

//parse a declaration statement
func (p *parser) declaration() Node {
	varTok := p.expect(TokenVar)
	declC := p.consumeComment()
	v := p.identifier()
	if p.peek().typ == TokenAsgn {
		p.expect(TokenAsgn)
		b := p.expression()
		return newDecl(p.position(varTok.pos), v, b, declC)
	} else {
		t := p.identifier()
		return newTypeDecl(p.position(varTok.pos), v, t, declC)
	}
}

//parse an expression
func (p *parser) expression() Node {
	switch p.peek().typ {
	case TokenIdent:
		p.next()
		switch p.peek().typ {
		case TokenLParen:
			p.backup()
			term := p.function(GlobalFunc)
			switch p.peek().typ {
			case TokenDot, TokenPipe, TokenAt, TokenVar, TokenIdent:
				return p.chain(term)
			default:
				return p.precedence(term, 0)
			}
		case TokenDot, TokenAt, TokenPipe, TokenVar, TokenIdent:
			p.backup()
			term := p.identifier()
			return p.chain(term)
		default:
			p.backup()
			return p.primaryExpr()
		}
	case TokenLambda:
		return p.lambda()
	case TokenLSBracket:
		return p.stringList()
	default:
		return p.primaryExpr()
	}
}

//parse a function or identifier invocation chain
// '|', '.' operators are left-associative.
func (p *parser) chain(lhs Node) Node {
	if t := p.peek().typ; t == TokenDot || t == TokenPipe || t == TokenAt {
		op := p.next()
		c := p.consumeComment()
		var ft FuncType
		switch op.typ {
		case TokenDot:
			ft = PropertyFunc
		case TokenPipe:
			ft = ChainFunc
		case TokenAt:
			ft = DynamicFunc
		}
		rhs := p.funcOrIdent(ft)
		return p.chain(newChain(p.position(op.pos), op.typ, lhs, rhs, c))
	}
	return lhs
}

func (p *parser) funcOrIdent(ft FuncType) (n Node) {
	p.next()
	if ft == ChainFunc || p.peek().typ == TokenLParen {
		p.backup()
		n = p.function(ft)
	} else {
		p.backup()
		n = p.identifier()
	}
	return
}

//parse an identifier
func (p *parser) identifier() *IdentifierNode {
	ident := p.expect(TokenIdent)
	n := newIdent(p.position(ident.pos), ident.val, p.consumeComment())
	return n
}

//parse a function call
func (p *parser) function(ft FuncType) Node {
	ident := p.expect(TokenIdent)
	c := p.consumeComment()
	p.expect(TokenLParen)
	args := p.parameters()
	p.expect(TokenRParen)
	multiLine := false
	if l := len(args); l > 0 {
		multiLine = p.hasNewLine(ident.pos, args[l-1].Position())
	}
	n := newFunc(p.position(ident.pos), ft, ident.val, args, multiLine, c)
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
	return p.expression()
}

//parse a string list
func (p *parser) stringList() Node {
	t := p.expect(TokenLSBracket)
	c := p.consumeComment()
	strings := make([]Node, 0, 10)
	for {
		if p.peek().typ == TokenRSBracket {
			break
		}
		strings = append(strings, p.stringItem())
		if p.next().typ != TokenComma {
			p.backup()
			break
		}
	}
	p.expect(TokenRSBracket)
	return newList(p.position(t.pos), strings, c)
}

func (p *parser) stringItem() (n Node) {
	switch t := p.peek(); t.typ {
	case TokenIdent:
		n = p.identifier()
	case TokenString:
		n = p.string()
	case TokenStar:
		n = p.star()
	default:
		p.unexpected(t, TokenIdent, TokenString, TokenStar)
	}
	return
}

func (p *parser) lambda() *LambdaNode {
	lambda := p.next()
	c := p.consumeComment()
	l := p.primaryExpr()
	return newLambda(p.position(lambda.pos), l, c)
}

// parse a primary expression
func (p *parser) primaryExpr() Node {
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
	for IsExprOperator(look.typ) && precedence[look.typ] >= minP {
		op := p.next()
		c := p.consumeComment()
		rhs := p.primary()
		look = p.peek()
		// left-associative
		for IsExprOperator(look.typ) && precedence[look.typ] > precedence[op.typ] {
			rhs = p.precedence(rhs, precedence[look.typ])
			look = p.peek()
		}

		multiLine := p.hasNewLine(lhs.Position(), rhs.Position())
		lhs = newBinary(p.position(op.pos), op.typ, lhs, rhs, multiLine, c)
	}
	return lhs
}

//parse a function call in a lambda expr
func (p *parser) lfunction() Node {
	ident := p.expect(TokenIdent)
	p.expect(TokenLParen)
	args := p.lparameters()
	p.expect(TokenRParen)

	multiLine := false
	if l := len(args); l > 0 {
		multiLine = p.hasNewLine(ident.pos, args[l-1].Position())
	}
	n := newFunc(p.position(ident.pos), GlobalFunc, ident.val, args, multiLine, nil)
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
	if IsExprOperator(p.peek().typ) {
		n = p.precedence(n, 0)
	}
	return
}

func (p *parser) primary() Node {
	switch tok := p.peek(); {
	case tok.typ == TokenLParen:
		p.next()
		c := p.consumeComment()
		n := p.primaryExpr()
		if b, ok := n.(*BinaryNode); ok {
			b.Parens = true
		}
		if commented, ok := n.(commentedNode); ok {
			commented.SetComment(c)
		}
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
	case tok.typ == TokenStar:
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
		return newUnary(p.position(tok.pos), tok.typ, p.primary(), p.consumeComment())
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
	num, err := newDur(p.position(token.pos), token.val, p.consumeComment())
	if err != nil {
		p.error(err)
	}
	return num
}

//parse a number literal
func (p *parser) number() Node {
	token := p.expect(TokenNumber)
	num, err := newNumber(p.position(token.pos), token.val, p.consumeComment())
	if err != nil {
		p.error(err)
	}
	return num
}

//parse a string literal
func (p *parser) string() Node {
	token := p.expect(TokenString)
	s := newString(p.position(token.pos), token.val, p.consumeComment())
	return s
}

//parse a regex literal
func (p *parser) regex() Node {
	token := p.expect(TokenRegex)
	r, err := newRegex(p.position(token.pos), token.val, p.consumeComment())
	if err != nil {
		p.error(err)
	}
	return r
}

// parse '*' literal
func (p *parser) star() Node {
	tok := p.expect(TokenStar)
	return newStar(p.position(tok.pos), p.consumeComment())
}

//parse a reference literal
func (p *parser) reference() Node {
	token := p.expect(TokenReference)
	r := newReference(p.position(token.pos), token.val, p.consumeComment())
	return r
}

func (p *parser) boolean() Node {
	n := p.next()
	num, err := newBool(p.position(n.pos), n.val, p.consumeComment())
	if err != nil {
		p.error(err)
	}
	return num
}
