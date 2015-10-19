package expr

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
	tokenIdent
	tokenNumber
	tokenString
	tokenLParen
	tokenRParen
	tokenComma
	tokenNot
	tokenTrue
	tokenFalse

	// begin operator tokens
	begin_tok_operator

	//begin mathematical operators
	begin_tok_operator_math

	tokenPlus
	tokenMinus
	tokenMult
	tokenDiv

	//end mathematical operators
	end_tok_operator_math

	// begin comparison operators
	begin_tok_operator_comp

	tokenEqual
	tokenNotEqual
	tokenLess
	tokenGreater
	tokenLessEqual
	tokenGreaterEqual
	tokenRegexEqual
	tokenRegexNotEqual

	//end comparison operators
	end_tok_operator_comp

	tokenAnd
	tokenOr

	//end operator tokens
	end_tok_operator
)

var operatorStr = [...]string{
	tokenPlus:          "+",
	tokenMinus:         "-",
	tokenMult:          "*",
	tokenDiv:           "/",
	tokenEqual:         "=",
	tokenNotEqual:      "!=",
	tokenLess:          "<",
	tokenGreater:       ">",
	tokenLessEqual:     "<=",
	tokenGreaterEqual:  ">=",
	tokenRegexEqual:    "=~",
	tokenRegexNotEqual: "!~",
	tokenAnd:           "AND",
	tokenOr:            "OR",
}

var strToOperator map[string]tokenType

func init() {
	strToOperator = make(map[string]tokenType, len(operatorStr))
	for t, s := range operatorStr {
		strToOperator[s] = tokenType(t)
	}
}

func (t tokenType) String() string {
	switch {
	case t == tokenError:
		return "ERR"
	case t == tokenEOF:
		return "EOF"
	case t == tokenIdent:
		return "identifier"
	case t == tokenNumber:
		return "number"
	case t == tokenString:
		return "literal"
	case t == tokenLParen:
		return "("
	case t == tokenRParen:
		return ")"
	case t == tokenComma:
		return ","
	case t == tokenNot:
		return "!"
	case isOperator(t):
		return operatorStr[t]
	}
	return "unknown"
}

func isOperator(typ tokenType) bool {
	return typ > begin_tok_operator && typ < end_tok_operator
}

func isMathOperator(typ tokenType) bool {
	return typ > begin_tok_operator_math && typ < end_tok_operator_math
}

func isCompOperator(typ tokenType) bool {
	return typ > begin_tok_operator_comp && typ < end_tok_operator_comp
}

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

// ----------------------
// Lex stateFns

func lexToken(l *lexer) stateFn {
	for {
		switch r := l.next(); {
		case isOperatorChar(r):
			l.backup()
			return lexOperator
		case unicode.IsDigit(r):
			l.backup()
			return lexNumber
		case unicode.IsLetter(r):
			return lexIdentOrKeyword
		case r == '"':
			l.backup()
			return lexQuotedIdent
		case r == '\'':
			l.backup()
			return lexString
		case r == '(':
			l.emit(tokenLParen)
			return lexToken
		case r == ')':
			l.emit(tokenRParen)
			return lexToken
		case r == ',':
			l.emit(tokenComma)
			return lexToken
		case isSpace(r):
			l.ignore()
		case r == eof:
			l.emit(tokenEOF)
			return nil
		default:
			return l.errorf("unknown state")
		}
	}
}

const operatorChars = "+-*/><!="

func isOperatorChar(r rune) bool {
	return strings.IndexRune(operatorChars, r) != -1
}

func lexOperator(l *lexer) stateFn {
	for {
		switch l.next() {
		case '+', '-', '*', '/':
			op := strToOperator[l.current()]
			l.emit(op)
			return lexToken
		case '>', '<', '!':
			if l.peek() == '=' {
				l.next()
			}
			op := strToOperator[l.current()]
			l.emit(op)
			return lexToken
		case '=':
			op := strToOperator[l.current()]
			l.emit(op)
			return lexToken
		}
	}
}

var keywords = map[string]tokenType{
	"and":   tokenAnd,
	"or":    tokenOr,
	"true":  tokenTrue,
	"false": tokenFalse,
}

func lexIdentOrKeyword(l *lexer) stateFn {
	for {
		switch r := l.next(); {
		case isValidIdent(r):
			//absorb
		default:
			l.backup()
			if t := keywords[strings.ToLower(l.current())]; t > 0 {
				l.emit(t)
			} else {
				l.emit(tokenIdent)
			}
			return lexToken
		}
	}
}

func lexQuotedIdent(l *lexer) stateFn {
	if n := l.next(); n != '"' {
		return l.errorf("unexpected %q expected '\"'", n)
	}
	for {
		switch r := l.next(); {
		case isValidIdent(r):
			//absorb
		case r == '"':
			l.emit(tokenIdent)
			return lexToken
		default:
			return l.errorf("invalid char in identifier %q", r)
		}
	}
}

// isValidIdent reports whether r is either a letter or a digit
func isValidIdent(r rune) bool {
	return unicode.IsDigit(r) || unicode.IsLetter(r) || r == '.' || r == '_'
}

func lexString(l *lexer) stateFn {
	if n := l.next(); n != '\'' {
		return l.errorf(`unexpected "%s" expected "'"`, n)
	}
	for {
		switch r := l.next(); {
		case r == '\\':
			if l.peek() == '\'' {
				l.next()
			}
		case r == '\'':
			l.emit(tokenString)
			return lexToken
		default:
			//absorb
		}
	}
}

// isSpace reports whether r is a space character.
func isSpace(r rune) bool {
	return unicode.IsSpace(r)
}

func lexNumber(l *lexer) stateFn {
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
		default:
			l.backup()
			l.emit(tokenNumber)
			return lexToken
		}
	}
}
