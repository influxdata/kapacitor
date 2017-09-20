package pipeline

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor/tick/ast"
)

// LambdaTick converts an ast.LambdaNode to TICKScript
func LambdaTick(l *ast.LambdaNode) string {
	var buf bytes.Buffer
	l.Format(&buf, "", false)
	lambda := buf.String()
	// If the lambda is a binary expression with many
	// lambdas, the lambda keyword shows up more than once.
	// This will remove them all to allow us to add it
	// in at the correct place.
	lambda = strings.Replace(lambda, "lambda: ", "", -1)
	return fmt.Sprintf("lambda: %s", lambda)
}

// SingleQuote converts string into a single-quoted string
func SingleQuote(s string) string {
	return fmt.Sprintf("'%s'", s)
}

// DurationTick converts duration into InfluxQL-style duration string
func DurationTick(dur time.Duration) string {
	return fmt.Sprintf("%s", influxql.FormatDuration(dur))
}

// BoolTick converts boolean into TICKScript boolean string
func BoolTick(b bool) string {
	return strings.ToUpper(fmt.Sprintf("%t", b))
}

func WithNode(buf *bytes.Buffer, name string, args ...interface{}) {
	buf.WriteString(fmt.Sprintf("|%s(", name))
	nodeArgs := make([]string, len(args))
	for i, arg := range args {
		switch a := arg.(type) {
		case string:
			nodeArgs[i] = SingleQuote(a)
		case time.Duration:
			nodeArgs[i] = DurationTick(a)
		case bool:
			nodeArgs[i] = BoolTick(a)
		}
	}

	buf.WriteString(strings.Join(nodeArgs, ", "))
	buf.WriteString(")")
}

// ChainIf adds chain method to buf if flag is true
func ChainIf(buf *bytes.Buffer, name string, flag bool) {
	if flag {
		Chain(buf, name)
	}
}

// Chain converts the name/value into a chaining method
func Chain(buf *bytes.Buffer, name string) {
	buf.Write([]byte(fmt.Sprintf(".%s()", name)))
}

// ChainInt converts the name/value into a chaining method
func ChainInt(buf *bytes.Buffer, name string, value int64) {
	if value != 0 {
		buf.Write([]byte(fmt.Sprintf(".%s(%d)", name, value)))
	}
}

// ChainFloat converts the name/value into a chaining method
func ChainFloat(buf *bytes.Buffer, name string, value float64) {
	if value != 0 {
		buf.Write([]byte(fmt.Sprintf(".%s(%f)", name, value)))
	}
}

// ChainString converts the name/value into a chaining method
func ChainString(buf *bytes.Buffer, name, value string) {
	if value != "" {
		buf.Write([]byte(fmt.Sprintf(".%s(%s)", name, SingleQuote(value))))
	}
}

// ChainLambda converts the name/value into a chaining method
func ChainLambda(buf *bytes.Buffer, name string, value *ast.LambdaNode) {
	if value != nil {
		buf.Write([]byte(fmt.Sprintf(".%s(%s)", name, LambdaTick(value))))
	}
}

// ChainDuration converts the name/value into a chaining method
func ChainDuration(buf *bytes.Buffer, name string, value time.Duration) {
	if value != 0 {
		buf.Write([]byte(fmt.Sprintf(".%s(%s)", name, DurationTick(value))))
	}
}
