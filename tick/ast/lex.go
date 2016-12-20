package ast

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type TokenType int

type stateFn func(*lexer) stateFn

const eof = -1

const (
	TokenError TokenType = iota
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
	TokenLSBracket
	TokenRSBracket
	TokenComma
	TokenNot
	TokenTrue
	TokenFalse
	TokenRegex
	TokenComment
	TokenStar

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

	// begin logical operators
	begin_tok_operator_logic

	TokenAnd
	TokenOr

	// end logical operators
	end_tok_operator_logic

	// begin comparison operators
	begin_tok_operator_comp

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

var strToOperator map[string]TokenType

const (
	KW_And    = "AND"
	KW_Or     = "OR"
	KW_True   = "TRUE"
	KW_False  = "FALSE"
	KW_Var    = "var"
	KW_Lambda = "lambda"
)

var keywords = map[string]TokenType{
	KW_And:    TokenAnd,
	KW_Or:     TokenOr,
	KW_True:   TokenTrue,
	KW_False:  TokenFalse,
	KW_Var:    TokenVar,
	KW_Lambda: TokenLambda,
}

func init() {
	strToOperator = make(map[string]TokenType, len(operatorStr))
	for t, s := range operatorStr {
		strToOperator[s] = TokenType(t)
	}
}

//String representation of an TokenType
func (t TokenType) String() string {
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
	case t == TokenLambda:
		return "lambda"
	case t == TokenNumber:
		return "number"
	case t == TokenString:
		return "string"
	case t == TokenRegex:
		return "regex"
	case t == TokenComment:
		return "//"
	case t == TokenStar:
		return "*"
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
	case t == TokenLSBracket:
		return "["
	case t == TokenRSBracket:
		return "]"
	case t == TokenComma:
		return ","
	case t == TokenNot:
		return "!"
	case t == TokenTrue:
		return "TRUE"
	case t == TokenFalse:
		return "FALSE"
	case IsExprOperator(t):
		return operatorStr[t]
	}
	return fmt.Sprintf("%d", t)
}

// True if token type is an operator used in mathematical or boolean expressions.
func IsExprOperator(typ TokenType) bool {
	return typ > begin_tok_operator && typ < end_tok_operator
}

// True if token type is an operator used in mathematical expressions.
func IsMathOperator(typ TokenType) bool {
	return typ > begin_tok_operator_math && typ < end_tok_operator_math
}

// True if token type is an operator used in comparisons.
func IsCompOperator(typ TokenType) bool {
	return typ > begin_tok_operator_comp && typ < end_tok_operator_comp
}

func IsLogicalOperator(typ TokenType) bool {
	return typ > begin_tok_operator_logic && typ < end_tok_operator_logic
}

// token represents a token or text string returned from the scanner.
type token struct {
	typ TokenType
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
func (l *lexer) emit(t TokenType) {
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
		case isUnaryOperatorChar(r):
			l.backup()
			return lexUnaryOperator
		case unicode.IsDigit(r), r == '-', r == '.':
			l.backup()
			return lexNumberOrDurationOrDot
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
			return tryLexBinaryOperator
		case r == '[':
			l.emit(TokenLSBracket)
			return lexToken
		case r == ']':
			l.emit(TokenRSBracket)
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
		case r == '*':
			l.emit(TokenStar)
			return lexToken
		case r == '/':
			if l.peek() == '/' {
				l.backup()
				return lexComment
			}
			l.backup()
			return lexRegex
		case r == eof:
			l.emit(TokenEOF)
			return nil
		default:
			l.errorf("unknown state, last char: %q", r)
			return nil
		}
	}
}

const unaryOperatorChars = "-!"

func isUnaryOperatorChar(r rune) bool {
	return strings.IndexRune(unaryOperatorChars, r) != -1
}

func lexUnaryOperator(l *lexer) stateFn {
	for {
		switch c := l.next(); c {
		case '-', '!':
			op := strToOperator[l.current()]
			l.emit(op)
			return lexToken
		default:
			return l.errorf("unexpected unary operator char %q", c)
		}
	}
}

func tryLexBinaryOperator(l *lexer) stateFn {
	l.ignoreSpace()
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
		default:
			// We didn't find an operator, bounce back up to lexToken
			l.backup()
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
					// 'lambda' is a valid identifier.
					l.backup()
					l.emit(TokenIdent)
				} else {
					l.emit(t)
				}
			} else {
				l.emit(TokenIdent)
			}
			return tryLexBinaryOperator
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

func lexNumberOrDurationOrDot(l *lexer) stateFn {
	foundDecimal := false
	first := true
	for {
		switch r := l.next(); {
		case r == '.':
			if first && !unicode.IsDigit(l.peek()) {
				l.emit(TokenDot)
				return lexToken
			}
			if foundDecimal {
				return l.errorf("multiple decimals in number")
			}
			foundDecimal = true
		case unicode.IsDigit(r):
			//absorb
		case !foundDecimal && isDurUnit(r):
			if r == 'm' && l.peek() == 's' {
				l.next()
			}
			l.emit(TokenDuration)
			return tryLexBinaryOperator
		default:
			l.backup()
			l.emit(TokenNumber)
			return tryLexBinaryOperator
		}
		first = false
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
			return tryLexBinaryOperator
		case eof:
			return l.errorf("unterminated field reference")
		}
	}
}

func lexSingleOrTripleString(l *lexer) stateFn {
	count := 0
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
					return tryLexBinaryOperator
				}
			}
			if (count == 1 && l.peek() != '\'') || count == 3 {
				total := count
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
							return tryLexBinaryOperator
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
		case r == eof:
			return l.errorf("unterminated regex")
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
		case r == '\n':
			// consume a contiguous block of spaces except newlines.
			n := l.next()
			for ; n != '\n' && isSpace(n); n = l.next() {
			}
			if n == '/' {
				// We still have more comment lines
				continue
			}
			l.backup()
			fallthrough
		case r == eof:
			l.emit(TokenComment)
			return lexToken
		}
	}
}
