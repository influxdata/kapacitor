package tick

import "github.com/yozora-hitagi/kapacitor/tick/ast"

// Formats a TICKscript according to the standard.
func Format(script string) (string, error) {
	root, err := ast.Parse(script)
	if err != nil {
		return "", err
	}
	return ast.Format(root), nil
}
