package storage

//go:generate easyjson -all ./version.go

import (
	"bytes"
	"encoding/json"
	"errors"
	"sync"

	"github.com/mailru/easyjson"
	"github.com/mailru/easyjson/jlexer"
	"github.com/mailru/easyjson/jwriter"
)

// VersionWrapper wraps a structure with a version so that changes
// to the structure can be properly decoded.
type VersionWrapper struct {
	Version int              `json:"version"`
	Value   *json.RawMessage `json:"value"`
}

// VersionJSONEncode encodes an object as json wrapping it in a VersionWrapper struct.
func VersionJSONEncode(version int, o interface{}) ([]byte, error) {
	if oo, ok := o.(easyjson.Marshaler); ok {
		return versionEasyJSONEncode(version, oo)
	}

	raw, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	rawCopy := make(json.RawMessage, len(raw))
	copy(rawCopy, raw)
	wrapper := VersionWrapper{
		Version: version,
		Value:   &rawCopy,
	}
	return json.Marshal(wrapper)
}

// VersionJSONDecode decodes and object that was encoded using VersionJSONEncode.
func VersionJSONDecode(data []byte, decF func(version int, dec *json.Decoder) error) error {
	var wrapper VersionWrapper
	err := json.Unmarshal(data, &wrapper)
	if err != nil {
		return err
	}
	if wrapper.Value == nil {
		return errors.New("empty value")
	}
	dec := json.NewDecoder(bytes.NewReader(*wrapper.Value))
	return decF(wrapper.Version, dec)
}

var (
	jwriterPool = sync.Pool{
		New: func() interface{} {
			return &jwriter.Writer{}
		},
	}
)

func versionEasyJSONEncode(version int, o easyjson.Marshaler) ([]byte, error) {
	i := jwriterPool.Get()
	defer jwriterPool.Put(i)
	w := i.(*jwriter.Writer)

	//w := &jwriter.Writer{}

	o.MarshalEasyJSON(w)
	//raw, err := json.Marshal(o)
	if w.Error != nil {
		return nil, w.Error
	}
	raw := json.RawMessage(w.Buffer.BuildBytes())
	wrapper := VersionWrapper{
		Version: version,
		Value:   &raw,
	}

	wrapper.MarshalEasyJSON(w)
	return w.BuildBytes()
}

// VersionEasyJSONDecode decodes and object that was encoded using VersionJSONEncode.
func VersionEasyJSONDecode(data []byte, decF func(version int, dec *jlexer.Lexer) error) error {
	var wrapper VersionWrapper
	err := wrapper.UnmarshalJSON(data)
	if err != nil {
		return err
	}
	if wrapper.Value == nil {
		return errors.New("empty value")
	}
	return decF(wrapper.Version, &jlexer.Lexer{Data: []byte(*wrapper.Value)})
}
