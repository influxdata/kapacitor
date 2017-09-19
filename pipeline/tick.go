package pipeline

import (
	"bytes"
	"fmt"
	"strings"

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
