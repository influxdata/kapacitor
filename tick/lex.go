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
	TokenError tokenType = iota
	TokenEOF
	TokenVar
	TokenAsgn
	TokenDot
	TokenPipe
	TokenAt
	TokenIdent
	TokenReference
	TokenLambda
	TokenNumber
	TokenString
	TokenDuration
	TokenLParen
	TokenRParen
	TokenComma
	TokenNot
	TokenTrue
	TokenFalse
	TokenRegex
	TokenComment

	// begin operator tokens
	begin_tok_operator

	//begin mathematical operators
	begin_tok_operator_math

	TokenPlus
	TokenMinus
	TokenMult
	TokenDiv
	TokenMod

	//end mathematical operators
	end_tok_operator_math

	// begin comparison operators
	begin_tok_operator_comp

	TokenAnd
	TokenOr
	TokenEqual
	TokenNotEqual
	TokenLess
	TokenGreater
	TokenLessEqual
	TokenGreaterEqual
	TokenRegexEqual
	TokenRegexNotEqual

	//end comparison operators
	end_tok_operator_comp

	//end operator tokens
	end_tok_operator
)

var operatorStr = [...]string{
	TokenNot:           "!",
	TokenPlus:          "+",
	TokenMinus:         "-",
	TokenMult:          "*",
	TokenDiv:           "/",
	TokenMod:           "%",
	TokenEqual:         "==",
	TokenNotEqual:      "!=",
	TokenLess:          "<",
	TokenGreater:       ">",
	TokenLessEqual:     "<=",
	TokenGreaterEqual:  ">=",
	TokenRegexEqual:    "=~",
	TokenRegexNotEqual: "!~",
	TokenAnd:           "AND",
	TokenOr:            "OR",
}

var strToOperator map[string]tokenType

const (
	KW_And    = "AND"
	KW_Or     = "OR"
	KW_True   = "TRUE"
	KW_False  = "FALSE"
	KW_Var    = "var"
	KW_Lambda = "lambda"
)

var keywords = map[string]tokenType{
	KW_And:    TokenAnd,
	KW_Or:     TokenOr,
	KW_True:   TokenTrue,
	KW_False:  TokenFalse,
	KW_Var:    TokenVar,
	KW_Lambda: TokenLambda,
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
	case t == TokenError:
		return "ERR"
	case t == TokenEOF:
		return "EOF"
	case t == TokenVar:
		return "var"
	case t == TokenIdent:
		return "identifier"
	case t == TokenReference:
		return "reference"
	case t == TokenDuration:
		return "duration"
	case t == TokenNumber:
		return "number"
	case t == TokenString:
		return "string"
	case t == TokenRegex:
		return "regex"
	case t == TokenComment:
		return "//"
	case t == TokenDot:
		return "."
	case t == TokenPipe:
		return "|"
	case t == TokenAt:
		return "@"
	case t == TokenAsgn:
		return "="
	case t == TokenLParen:
		return "("
	case t == TokenRParen:
		return ")"
	case t == TokenComma:
		return ","
	case t == TokenNot:
		return "!"
	case t == TokenTrue:
		return "TRUE"
	case t == TokenFalse:
		return "FALSE"
	case isExprOperator(t):
		return operatorStr[t]
	}
	return fmt.Sprintf("%d", t)
}

// True if token type is an operator used in mathematical or boolean expressions.
func isExprOperator(typ tokenType) bool {
	return typ > begin_tok_operator && typ < end_tok_operator
}

// True if token type is an operator used in mathematical expressions.
func isMathOperator(typ tokenType) bool {
	return typ > begin_tok_operator_math && typ < end_tok_operator_math
}

// True if token type is an operator used in comparisions.
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
	l.tokens <- token{TokenError, l.start, fmt.Sprintf(format, args...)}
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
			l.emit(TokenLParen)
			return lexToken
		case r == ')':
			l.emit(TokenRParen)
			return lexToken
		case r == '.':
			l.emit(TokenDot)
			return lexToken
		case r == '|':
			l.emit(TokenPipe)
			return lexToken
		case r == '@':
			l.emit(TokenAt)
			return lexToken
		case r == ',':
			l.emit(TokenComma)
			return lexToken
		case r == eof:
			l.emit(TokenEOF)
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
			if op == TokenRegexNotEqual {
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
				if op == TokenRegexEqual {
					l.ignoreSpace()
					if l.peek() == '/' {
						return lexRegex
					}
				}
			} else {
				l.emit(TokenAsgn)
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
				if t == TokenLambda && l.next() != ':' {
					return l.errorf("missing ':' on lambda keyword")
				}
				l.emit(t)
			} else {
				l.emit(TokenIdent)
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
			l.emit(TokenDuration)
			return lexToken
		default:
			l.backup()
			l.emit(TokenNumber)
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
			l.emit(TokenReference)
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
					l.emit(TokenString)
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
							l.emit(TokenString)
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
			l.emit(TokenRegex)
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
			l.emit(TokenComment)
			return lexToken
		}
	}
}
