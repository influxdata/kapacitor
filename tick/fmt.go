package tick

import "bytes"

// Indent string for formatted TICKscripts
const indentStep = "    "

// Formats a TICKscript according to the standard.
func Format(script string) (string, error) {
	root, err := parse(script)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	buf.Grow(len(script))
	root.Format(&buf, "", false)
	return buf.String(), nil
}
