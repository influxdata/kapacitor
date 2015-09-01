package dsl

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type tokenType int

type stateFn func(*lexer) stateFn

const eof = -1

const (
	tokenError tokenType = iota
	tokenEOF
	tokenVar
	tokenIdent
	tokenAsgn
	tokenDot
	tokenLParen
	tokenRParen
	tokenComma
	tokenNumber
	tokenDuration
	tokenString
	tokenSColon
)

// token represents a token or text string returned from the scanner.
type token struct {
	typ tokenType
	pos int
	val string
}

// lexer holds the state of the scanner.
type lexer struct {
	input  string     // the string being scanned.
	start  int        // start position of this token.
	pos    int        // current position in the input.
	width  int        // width of last rune read from input.
	tokens chan token // channel of scanned tokens.
}

//String representation of an tokenType
func (t tokenType) String() string {
	switch t {
	case tokenError:
		return "Error"
	case tokenEOF:
		return "EOF"
	case tokenVar:
		return "var"
	case tokenIdent:
		return "identifier"
	case tokenAsgn:
		return "="
	case tokenDot:
		return "."
	case tokenLParen:
		return "("
	case tokenRParen:
		return "("
	case tokenComma:
		return ","
	case tokenNumber:
		return "number"
	case tokenString:
		return "string"
	case tokenSColon:
		return ";"
	}
	return "unknow type"
}

//String representation of an token
func (t token) String() string {
	switch t.typ {
	case tokenError:
		return t.val
	case tokenEOF:
		return "EOF"
	}
	if len(t.val) > 10 {
		return fmt.Sprintf("%.10q...", t.val)
	}
	return fmt.Sprintf("%q", t.val)
}

func lex(input string) *lexer {
	l := &lexer{
		input:  input,
		tokens: make(chan token),
	}

	go l.run()
	return l
}

// run lexes the input by executing state functions until
// the state is nil.
func (l *lexer) run() {
	for state := lexToken; state != nil; {
		state = state(l)
	}
	close(l.tokens)
}

// emit passes an token back to the client.
func (l *lexer) emit(t tokenType) {
	l.tokens <- token{t, l.start, l.current()}
	l.start = l.pos
}

// nextToken returns the next token from the input.
// The second value is false when there are no more tokens
func (l *lexer) nextToken() (token, bool) {
	tok, closed := <-l.tokens
	return tok, closed
}

// lineNumber reports which line number and start of line position of a given position is on in the input
func (l *lexer) lineNumber(pos int) (int, int) {
	line := 1 + strings.Count(l.input[:pos], "\n")
	i := strings.LastIndex(l.input[:pos], "\n")
	return line, pos - i
}

// next returns the next rune in the input.
func (l *lexer) next() (r rune) {
	if l.pos >= len(l.input) {
		l.width = 0
		return eof
	}
	r, l.width =
		utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += l.width
	return
}

// errorf returns an error token and terminates the scan by passing
// back a nil pointer that will be the next state, terminating l.nextToken.
func (l *lexer) errorf(format string, args ...interface{}) stateFn {
	l.tokens <- token{tokenError, l.start, fmt.Sprintf(format, args...)}
	return nil
}

//Backup the lexer to the previous rune
func (l *lexer) backup() {
	l.pos -= l.width
}

// peek returns but does not consume the next rune in the input.
func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

//Backup the lexer to the previous rune
func (l *lexer) current() string {
	return l.input[l.start:l.pos]
}

// ignore skips over the pending input before this point.
func (l *lexer) ignore() {
	l.start = l.pos
}

// expect the next rune to be r
func (l *lexer) expect(r rune) bool {
	if l.peek() == r {
		l.next()
		return true
	}
	return false
}

func lexToken(l *lexer) stateFn {
	for {
		switch r := l.next(); {
		case isSymbol(r):
			return lexSymbol
		case unicode.IsDigit(r):
			l.backup()
			return lexNumberOrDuration
		case r == 'v':
			return lexVarOrIdent
		case unicode.IsLetter(r):
			return lexIdent
		case r == '"':
			return lexString
		case isSpace(r):
			l.ignore()
		case r == '/':
			return lexComment
		case r == eof:
			l.emit(tokenEOF)
			return nil
		default:
			l.errorf("unknown state")
			return nil
		}
	}
}

const symbols = "=().;,"

func isSymbol(r rune) bool {
	return strings.IndexRune(symbols, r) != -1
}

func lexSymbol(l *lexer) stateFn {
	s := l.current()
	switch s {
	case "=":
		l.emit(tokenAsgn)
	case "(":
		l.emit(tokenLParen)
	case ")":
		l.emit(tokenRParen)
	case ".":
		l.emit(tokenDot)
	case ";":
		l.emit(tokenSColon)
	case ",":
		l.emit(tokenComma)
	}
	return lexToken
}

func lexVarOrIdent(l *lexer) stateFn {
	if l.expect('a') && l.expect('r') && isSpace(l.peek()) {
		l.emit(tokenVar)
		return lexToken
	}

	return lexIdent
}

func lexIdent(l *lexer) stateFn {
	for {
		switch r := l.next(); {
		case isValidIdent(r):
			//absorb
		default:
			l.backup()
			l.emit(tokenIdent)
			return lexToken
		}
	}
}

// isValidIdent reports whether r is either a letter or a digit
func isValidIdent(r rune) bool {
	return unicode.IsDigit(r) || unicode.IsLetter(r)
}

// isSpace reports whether r is a space character.
func isSpace(r rune) bool {
	return unicode.IsSpace(r)
}

const durationUnits = "smh"

func isDurUnit(r rune) bool {
	return strings.IndexRune(durationUnits, r) != -1
}

func lexNumberOrDuration(l *lexer) stateFn {
	foundDecimal := false

	for {
		switch r := l.next(); {
		case r == '.':
			if foundDecimal {
				return l.errorf("multiple decimals in number")
			}
			foundDecimal = true
		case unicode.IsDigit(r):
			//absorb
		case isDurUnit(r):
			l.emit(tokenDuration)
			return lexToken
		default:
			l.backup()
			l.emit(tokenNumber)
			return lexToken
		}
	}
}

func lexString(l *lexer) stateFn {
	for {
		switch r := l.next(); {
		case r == '"':
			l.emit(tokenString)
			return lexToken
		case r == eof:
			return l.errorf("unterminated string")
		}
	}
}

func lexComment(l *lexer) stateFn {
	if !l.expect('/') {
		return l.errorf("invalid character '/'")
	}

	for {
		switch r := l.next(); {
		case r == '\n' || r == eof:
			l.ignore()
			return lexToken
		}
	}
}
