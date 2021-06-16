package istrings

import (
	"strings"
)

// Builder is a thin wrapper around strings.Builder that automatically interns strings
type Builder struct {
	strings.Builder
}

// IString returns the IString for a builder
func (b *Builder) IString() IString {
	return Get(b.Builder.String())
}

// WriteIString returns the WriteIString for a builder
func (b *Builder) WriteIString(is IString) (int, error) {
	s := is.String()
	return b.Builder.WriteString(s)
}
