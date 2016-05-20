package tick

import (
	"bytes"

	"github.com/influxdata/kapacitor/tick/ast"
)

// Formats a TICKscript according to the standard.
func Format(script string) (string, error) {
	root, err := ast.Parse(script)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	buf.Grow(len(script))
	root.Format(&buf, "", false)
	return buf.String(), nil
}
