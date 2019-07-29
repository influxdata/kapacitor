package storage

import (
	"bytes"
	"encoding/json"
	"errors"
)

// VersionWrapper wraps a structure with a version so that changes
// to the structure can be properly decoded.
type VersionWrapper struct {
	Version int              `json:"version"`
	Value   *json.RawMessage `json:"value"`
}

// VersionJSONEncode encodes an object as json wrapping it in a VersionWrapper struct.
func VersionJSONEncode(version int, o interface{}) ([]byte, error) {
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
