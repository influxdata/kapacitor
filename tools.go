// Tools is for building out tools so mod can version them, add any tools you need at runtime to this file as _ imports.
// This is to follow along with best practices for mod.  https://github.com/golang/go/issues/25922

//go:build tools
// +build tools

package kapacitor

import (
	_ "github.com/benbjohnson/tmpl"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"

	// so we can use the rust dependencies of flux
	_ "github.com/influxdata/pkg-config"
	_ "github.com/mailru/easyjson/easyjson"
)
