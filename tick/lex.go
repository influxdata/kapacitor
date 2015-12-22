package tick

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
	tokenAsgn
	tokenDot
	tokenIdent
	tokenReference
	tokenLambda
	tokenNumber
	tokenString
	tokenDuration
	tokenLParen
	tokenRParen
	tokenComma
	tokenNot
	tokenTrue
	tokenFalse
	tokenRegex

	// begin operator tokens
	begin_tok_operator

	//begin mathematical operators
	begin_tok_operator_math

	tokenPlus
	tokenMinus
	tokenMult
	tokenDiv
	tokenMod

	//end mathematical operators
	end_tok_operator_math

	// begin comparison operators
	begin_tok_operator_comp

	tokenAnd
	tokenOr
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

	//end operator tokens
	end_tok_operator
)

var operatorStr = [...]string{
	tokenNot:           "!",
	tokenPlus:          "+",
	tokenMinus:         "-",
	tokenMult:          "*",
	tokenDiv:           "/",
	tokenMod:           "%",
	tokenEqual:         "==",
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

var keywords = map[string]tokenType{
	"AND":    tokenAnd,
	"OR":     tokenOr,
	"TRUE":   tokenTrue,
	"FALSE":  tokenFalse,
	"var":    tokenVar,
	"lambda": tokenLambda,
}

func init() {
	strToOperator = make(map[string]tokenType, len(operatorStr))
	for t, s := range operatorStr {
		strToOperator[s] = tokenType(t)
	}
}

//String representation of an tokenType
func (t tokenType) String() string {
	switch {
	case t == tokenError:
		return "ERR"
	case t == tokenEOF:
		return "EOF"
	case t == tokenVar:
		return "var"
	case t == tokenIdent:
		return "identifier"
	case t == tokenReference:
		return "reference"
	case t == tokenDuration:
		return "duration"
	case t == tokenNumber:
		return "number"
	case t == tokenString:
		return "string"
	case t == tokenRegex:
		return "regex"
	case t == tokenDot:
		return "."
	case t == tokenAsgn:
		return "="
	case t == tokenLParen:
		return "("
	case t == tokenRParen:
		return ")"
	case t == tokenComma:
		return ","
	case t == tokenNot:
		return "!"
	case t == tokenTrue:
		return "TRUE"
	case t == tokenFalse:
		return "FALSE"
	case isOperator(t):
		return operatorStr[t]
	}
	return fmt.Sprintf("%d", t)
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

func (t token) String() string {
	return fmt.Sprintf("{%v pos: %d val: %s}", t.typ, t.pos, t.val)
}

// lexer holds the state of the scanner.
type lexer struct {
	input  string     // the string being scanned.
	start  int        // start position of this token.
	pos    int        // current position in the input.
	width  int        // width of last rune read from input.
	tokens chan token // channel of scanned tokens.
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

// ignore a contiguous block of spaces.
func (l *lexer) ignoreSpace() {
	for isSpace(l.next()) {
		l.ignore()
	}
	l.backup()
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
		case isOperatorChar(r):
			l.backup()
			return lexOperator
		case unicode.IsDigit(r), r == '-':
			l.backup()
			return lexNumberOrDuration
		case unicode.IsLetter(r):
			return lexIdentOrKeyword
		case r == '"':
			return lexReference
		case r == '\'':
			l.backup()
			return lexSingleOrTripleString
		case isSpace(r):
			l.ignore()
		case r == '(':
			l.emit(tokenLParen)
			return lexToken
		case r == ')':
			l.emit(tokenRParen)
			return lexToken
		case r == '.':
			l.emit(tokenDot)
			return lexToken
		case r == ',':
			l.emit(tokenComma)
			return lexToken
		case r == eof:
			l.emit(tokenEOF)
			return nil
		default:
			l.errorf("unknown state")
			return nil
		}
	}
}

const operatorChars = "+-*/><!=%"

func isOperatorChar(r rune) bool {
	return strings.IndexRune(operatorChars, r) != -1
}

func lexOperator(l *lexer) stateFn {
	for {
		switch l.next() {
		case '+', '-', '*', '%':
			op := strToOperator[l.current()]
			l.emit(op)
			return lexToken
		case '/':
			if l.peek() == '/' {
				l.backup()
				return lexComment
			}
			op := strToOperator[l.current()]
			l.emit(op)
			return lexToken
		case '!':
			if l.peek() == '=' || l.peek() == '~' {
				l.next()
			}
			op := strToOperator[l.current()]
			l.emit(op)
			if op == tokenRegexNotEqual {
				l.ignoreSpace()
				if l.peek() == '/' {
					return lexRegex
				}
			}
			return lexToken
		case '>', '<':
			if l.peek() == '=' {
				l.next()
			}
			op := strToOperator[l.current()]
			l.emit(op)
			return lexToken
		case '=':
			if l.peek() == '~' || l.peek() == '=' {
				l.next()
				op := strToOperator[l.current()]
				l.emit(op)
				if op == tokenRegexEqual {
					l.ignoreSpace()
					if l.peek() == '/' {
						return lexRegex
					}
				}
			} else {
				l.emit(tokenAsgn)
				l.ignoreSpace()
				if l.peek() == '/' {
					return lexRegex
				}
			}
			return lexToken
		}
	}
}

func lexIdentOrKeyword(l *lexer) stateFn {
	for {
		switch r := l.next(); {
		case isValidIdent(r):
			//absorb
		default:
			l.backup()
			if t := keywords[l.current()]; t > 0 {
				if t == tokenLambda && l.next() != ':' {
					return l.errorf("missing ':' on lambda keyword")
				}
				l.emit(t)
			} else {
				l.emit(tokenIdent)
			}
			return lexToken
		}
	}
}

// isValidIdent reports whether r is either a letter or a digit
func isValidIdent(r rune) bool {
	return unicode.IsDigit(r) || unicode.IsLetter(r) || r == '_'
}

// isSpace reports whether r is a space character.
func isSpace(r rune) bool {
	return unicode.IsSpace(r)
}

const durationUnits = "uµsmhdw"

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
			if r == 'm' && l.peek() == 's' {
				l.next()
			}
			l.emit(tokenDuration)
			return lexToken
		default:
			l.backup()
			l.emit(tokenNumber)
			return lexToken
		}
	}
}

func lexReference(l *lexer) stateFn {
	for {
		switch r := l.next(); r {
		case '\\':
			if l.peek() == '"' {
				l.next()
			}
		case '"':
			l.emit(tokenReference)
			return lexToken
		case eof:
			return l.errorf("unterminated field reference")
		}
	}
}

func lexSingleOrTripleString(l *lexer) stateFn {
	count := 0
	total := 0
	for {
		switch r := l.next(); r {
		case '\'':
			count++
			if count == 1 && l.peek() == '\'' {
				// check for empty '' string
				count++
				l.next()
				if l.peek() == '\'' {
					count++
					l.next()
				} else {
					l.emit(tokenString)
					return lexToken
				}
			}
			if (count == 1 && l.peek() != '\'') || count == 3 {
				total = count
				for {
					switch r := l.next(); {
					//escape single quotes if single quoted
					case r == '\\' && count == 1:
						if l.peek() == '\'' {
							l.next()
						}
					case r == '\'':
						count--
						if count == 0 {
							l.emit(tokenString)
							return lexToken
						}
					case r == eof:
						return l.errorf("unterminated string")
					default:
						count = total
					}
				}
			}
		default:
			return l.errorf("unterminated string")
		}
	}
}

func lexRegex(l *lexer) stateFn {
	if n := l.next(); n != '/' {
		return l.errorf(`unexpected "%c" expected "/"`, n)
	}
	for {
		switch r := l.next(); {
		case r == '\\':
			if l.peek() == '/' {
				l.next()
			}
		case r == '/':
			l.emit(tokenRegex)
			return lexToken
		default:
			//absorb
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
