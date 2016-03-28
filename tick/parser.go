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

func (p *parser) hasNewLine(start, end int) bool {
	return strings.IndexRune(p.Text[start:end], '\n') != -1
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
	l := newList(p.position(p.peek().pos))
	var s, prevc, nextc Node
	for {
		switch p.peek().typ {
		case TokenEOF:
			if prevc != nil {
				l.Add(prevc)
			}
			return l
		default:
			s, nextc = p.statement(prevc)
			l.Add(s)
		}
		prevc = nextc
	}
}

//parse a statement
func (p *parser) statement(c Node) (Node, Node) {
	if c == nil {
		c = p.comment()
	}
	switch t := p.peek().typ; t {
	case TokenVar:
		return p.declaration(c)
	default:
		return p.expression(c)
	}
}

//parse a declaration statement
func (p *parser) declaration(c Node) (Node, Node) {
	if c == nil {
		c = p.comment()
	}
	v := p.vr()
	op := p.expect(TokenAsgn)
	b, extra := p.expression(nil)
	return newDecl(p.position(op.pos), v, b, c), extra
}

//parse a 'var ident' expression
func (p *parser) vr() *IdentifierNode {
	p.expect(TokenVar)
	ident := p.expect(TokenIdent)
	return newIdent(p.position(ident.pos), ident.val, nil)
}

//parse an expression
func (p *parser) expression(c Node) (Node, Node) {
	if c == nil {
		c = p.comment()
	}
	switch p.peek().typ {
	case TokenIdent:
		term := p.funcOrIdent(globalFunc, c)
		return p.chain(term)
	default:
		return p.primary(c), nil
	}
}

//parse a function or identifier invocation chain
// '|', '.' operators are left-associative.
func (p *parser) chain(lhs Node) (Node, Node) {
	c := p.comment()
	if t := p.peek().typ; t == TokenDot || t == TokenPipe || t == TokenAt {
		op := p.next()
		var ft funcType
		switch op.typ {
		case TokenDot:
			ft = propertyFunc
		case TokenPipe:
			ft = chainFunc
		case TokenAt:
			ft = dynamicFunc
		}
		rhs := p.funcOrIdent(ft, nil)
		return p.chain(newChain(p.position(op.pos), op.typ, lhs, rhs, c))
	}
	return lhs, c
}

func (p *parser) funcOrIdent(ft funcType, c Node) (n Node) {
	p.next()
	if ft == chainFunc || p.peek().typ == TokenLParen {
		p.backup()
		n = p.function(ft, c)
	} else {
		p.backup()
		n = p.identifier(c)
	}
	return
}

//parse an identifier
func (p *parser) identifier(c Node) Node {
	ident := p.expect(TokenIdent)
	n := newIdent(p.position(ident.pos), ident.val, c)
	return n
}

//parse a function call
func (p *parser) function(ft funcType, c Node) Node {
	ident := p.expect(TokenIdent)
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
	c := p.comment()
	switch p.peek().typ {
	case TokenIdent:
		n, _ = p.expression(c)
	case TokenLambda:
		lambda := p.next()
		l := p.lambdaExpr(nil)
		n = newLambda(p.position(lambda.pos), l, c)
	default:
		n = p.primary(c)
	}
	return
}

// parse the lambda expression.
func (p *parser) lambdaExpr(c Node) Node {
	return p.precedence(p.primary(nil), 0, c)
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
func (p *parser) precedence(lhs Node, minP int, c Node) Node {
	look := p.peek()
	for isExprOperator(look.typ) && precedence[look.typ] >= minP {
		op := p.next()
		rhs := p.primary(nil)
		look = p.peek()
		// right-associative
		for isExprOperator(look.typ) && precedence[look.typ] >= precedence[op.typ] {
			rhs = p.precedence(rhs, precedence[look.typ], nil)
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
	n := newFunc(p.position(ident.pos), globalFunc, ident.val, args, multiLine, nil)
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
	n = p.primary(nil)
	if isExprOperator(p.peek().typ) {
		n = p.precedence(n, 0, nil)
	}
	return
}

// parse a primary expression
func (p *parser) primary(c Node) Node {
	if c == nil {
		c = p.comment()
	}
	switch tok := p.peek(); {
	case tok.typ == TokenLParen:
		p.next()
		n := p.lambdaExpr(c)
		if b, ok := n.(*BinaryNode); ok {
			b.Parens = true
		}
		p.expect(TokenRParen)
		return n
	case tok.typ == TokenNumber:
		return p.number(c)
	case tok.typ == TokenString:
		return p.string(c)
	case tok.typ == TokenTrue, tok.typ == TokenFalse:
		return p.boolean(c)
	case tok.typ == TokenDuration:
		return p.duration(c)
	case tok.typ == TokenRegex:
		return p.regex(c)
	case tok.typ == TokenMult:
		return p.star(c)
	case tok.typ == TokenReference:
		return p.reference(c)
	case tok.typ == TokenIdent:
		p.next()
		if p.peek().typ == TokenLParen {
			p.backup()
			return p.lfunction()
		}
		p.backup()
		return p.identifier(c)
	case tok.typ == TokenMinus, tok.typ == TokenNot:
		p.next()
		return newUnary(p.position(tok.pos), tok.typ, p.primary(nil), c)
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
func (p *parser) duration(c Node) Node {
	token := p.expect(TokenDuration)
	num, err := newDur(p.position(token.pos), token.val, c)
	if err != nil {
		p.error(err)
	}
	return num
}

//parse a number literal
func (p *parser) number(c Node) Node {
	token := p.expect(TokenNumber)
	num, err := newNumber(p.position(token.pos), token.val, c)
	if err != nil {
		p.error(err)
	}
	return num
}

//parse a string literal
func (p *parser) string(c Node) Node {
	token := p.expect(TokenString)
	s := newString(p.position(token.pos), token.val, c)
	return s
}

//parse a regex literal
func (p *parser) regex(c Node) Node {
	token := p.expect(TokenRegex)
	r, err := newRegex(p.position(token.pos), token.val, c)
	if err != nil {
		p.error(err)
	}
	return r
}

// parse '*' literal
func (p *parser) star(c Node) Node {
	tok := p.expect(TokenMult)
	return newStar(p.position(tok.pos), c)
}

//parse a reference literal
func (p *parser) reference(c Node) Node {
	token := p.expect(TokenReference)
	r := newReference(p.position(token.pos), token.val, c)
	return r
}

func (p *parser) boolean(c Node) Node {
	n := p.next()
	num, err := newBool(p.position(n.pos), n.val, c)
	if err != nil {
		p.error(err)
	}
	return num
}

func (p *parser) comment() Node {
	var comments []string
	pos := -1
	for p.peek().typ == TokenComment {
		n := p.next()
		if pos == -1 {
			pos = n.pos
		}
		comments = append(comments, n.val)
	}
	if len(comments) > 0 {
		return newComment(p.position(pos), comments)
	}
	return nil
}
