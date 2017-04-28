package listmap

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/BurntSushi/toml"
	"github.com/pkg/errors"
)

// DoUnmarshalTOML unmarshals either a list of maps or just a single map into dst.
// The argument dst must be a pointer to a slice.
func DoUnmarshalTOML(dst, src interface{}) error {
	dstV := reflect.Indirect(reflect.ValueOf(dst))
	if !dstV.CanSet() {
		return errors.New("dst must be settable")
	}
	dstK := dstV.Kind()
	if dstK != reflect.Slice {
		return errors.New("dst must be a slice")
	}

	srcV := reflect.ValueOf(src)
	srcK := srcV.Kind()

	var srvValues []reflect.Value
	switch srcK {
	case reflect.Slice:
		l := srcV.Len()
		srvValues = make([]reflect.Value, l)
		for i := 0; i < l; i++ {
			srvValues[i] = srcV.Index(i)
		}
	case reflect.Map:
		srvValues = []reflect.Value{srcV}
	default:
		return fmt.Errorf("src must be a slice or map, got %v", srcK)
	}

	// We want to preserve the TOML decoding behavior exactly,
	// so we first re-encode the src data and then decode again,
	// only this time directly into the element of the slice.
	var buf bytes.Buffer
	dstV.Set(reflect.MakeSlice(dstV.Type(), len(srvValues), len(srvValues)))
	for i, v := range srvValues {
		if err := toml.NewEncoder(&buf).Encode(v.Interface()); err != nil {
			return errors.Wrap(err, "failed to reencode toml data")
		}
		newValue := reflect.New(dstV.Type().Elem())
		if _, err := toml.Decode(buf.String(), newValue.Interface()); err != nil {
			return err
		}
		dstV.Index(i).Set(reflect.Indirect(newValue))
		buf.Reset()
	}
	return nil
}
