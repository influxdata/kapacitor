//+build tools

package tools

import (
	_ "github.com/benbjohnson/tmpl"              // vendoring for generate
	_ "github.com/golang/protobuf/protoc-gen-go" // vendoring for generate
	_ "github.com/mailru/easyjson/easyjson"      // vendoring for generate
)
