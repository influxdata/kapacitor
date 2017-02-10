package tsm1

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/pkg/pool"
	"github.com/influxdata/influxdb/tsdb"
)

const (
	// BlockFloat64 designates a block encodes float64 values.
	BlockFloat64 = byte(0)

	// BlockInteger designates a block encodes int64 values.
	BlockInteger = byte(1)

	// BlockBoolean designates a block encodes boolean values.
	BlockBoolean = byte(2)

	// BlockString designates a block encodes string values.
	BlockString = byte(3)

	// encodedBlockHeaderSize is the size of the header for an encoded block.  There is one
	// byte encoding the type of the block.
	encodedBlockHeaderSize = 1
)

func init() {
	// Prime the pools with one encoder/decoder for each available CPU.
	vals := make([]interface{}, 0, runtime.NumCPU())
	for _, p := range []*pool.Generic{
		timeEncoderPool, timeDecoderPool,
		integerEncoderPool, integerDecoderPool,
		floatDecoderPool, floatDecoderPool,
		stringEncoderPool, stringEncoderPool,
		booleanEncoderPool, booleanDecoderPool,
	} {
		vals = vals[:0]
		// Check one out to force the allocation now and hold onto it
		for i := 0; i < runtime.NumCPU(); i++ {
			v := p.Get(tsdb.DefaultMaxPointsPerBlock)
			vals = append(vals, v)
		}
		// Add them all back
		for _, v := range vals {
			p.Put(v)
		}
	}
}

var (
	// encoder pools

	timeEncoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return NewTimeEncoder(sz)
	})
	integerEncoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return NewIntegerEncoder(sz)
	})
	floatEncoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return NewFloatEncoder()
	})
	stringEncoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return NewStringEncoder(sz)
	})
	booleanEncoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return NewBooleanEncoder(sz)
	})

	// decoder pools

	timeDecoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return &TimeDecoder{}
	})
	integerDecoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return &IntegerDecoder{}
	})
	floatDecoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return &FloatDecoder{}
	})
	stringDecoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return &StringDecoder{}
	})
	booleanDecoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return &BooleanDecoder{}
	})
)

// Value represents a TSM-encoded value.
type Value interface {
	// UnixNano returns the timestamp of the value in nanoseconds since unix epoch.
	UnixNano() int64

	// Value returns the underlying value.
	Value() interface{}

	// Size returns the number of bytes necessary to represent the value and its timestamp.
	Size() int

	// String returns the string representation of the value and its timestamp.
	String() string

	// internalOnly is unexported to ensure implementations of Value
	// can only originate in this package.
	internalOnly()
}

// NewValue returns a new Value with the underlying type dependent on value.
func NewValue(t int64, value interface{}) Value {
	switch v := value.(type) {
	case int64:
		return IntegerValue{unixnano: t, value: v}
	case float64:
		return FloatValue{unixnano: t, value: v}
	case bool:
		return BooleanValue{unixnano: t, value: v}
	case string:
		return StringValue{unixnano: t, value: v}
	}
	return EmptyValue{}
}

// NewIntegerValue returns a new integer value.
func NewIntegerValue(t int64, v int64) Value {
	return IntegerValue{unixnano: t, value: v}
}

// NewFloatValue returns a new float value.
func NewFloatValue(t int64, v float64) Value {
	return FloatValue{unixnano: t, value: v}
}

// NewBooleanValue returns a new boolean value.
func NewBooleanValue(t int64, v bool) Value {
	return BooleanValue{unixnano: t, value: v}
}

// NewStringValue returns a new string value.
func NewStringValue(t int64, v string) Value {
	return StringValue{unixnano: t, value: v}
}

// EmptyValue is used when there is no appropriate other value.
type EmptyValue struct{}

// UnixNano returns tsdb.EOF.
func (e EmptyValue) UnixNano() int64 { return tsdb.EOF }

// Value returns nil.
func (e EmptyValue) Value() interface{} { return nil }

// Size returns 0.
func (e EmptyValue) Size() int { return 0 }

// String returns the empty string.
func (e EmptyValue) String() string { return "" }

func (_ EmptyValue) internalOnly()   {}
func (_ StringValue) internalOnly()  {}
func (_ IntegerValue) internalOnly() {}
func (_ BooleanValue) internalOnly() {}
func (_ FloatValue) internalOnly()   {}

// Encode converts the values to a byte slice.  If there are no values,
// this function panics.
func (a Values) Encode(buf []byte) ([]byte, error) {
	if len(a) == 0 {
		panic("unable to encode block type")
	}

	switch a[0].(type) {
	case FloatValue:
		return encodeFloatBlock(buf, a)
	case IntegerValue:
		return encodeIntegerBlock(buf, a)
	case BooleanValue:
		return encodeBooleanBlock(buf, a)
	case StringValue:
		return encodeStringBlock(buf, a)
	}

	return nil, fmt.Errorf("unsupported value type %T", a[0])
}

// InfluxQLType returns the influxql.DataType the values map to.
func (a Values) InfluxQLType() (influxql.DataType, error) {
	if len(a) == 0 {
		return influxql.Unknown, fmt.Errorf("no values to infer type")
	}

	switch a[0].(type) {
	case FloatValue:
		return influxql.Float, nil
	case IntegerValue:
		return influxql.Integer, nil
	case BooleanValue:
		return influxql.Boolean, nil
	case StringValue:
		return influxql.String, nil
	}

	return influxql.Unknown, fmt.Errorf("unsupported value type %T", a[0])
}

// BlockType returns the type of value encoded in a block or an error
// if the block type is unknown.
func BlockType(block []byte) (byte, error) {
	blockType := block[0]
	switch blockType {
	case BlockFloat64, BlockInteger, BlockBoolean, BlockString:
		return blockType, nil
	default:
		return 0, fmt.Errorf("unknown block type: %d", blockType)
	}
}

// BlockCount returns the number of timestamps encoded in block.
func BlockCount(block []byte) int {
	if len(block) <= encodedBlockHeaderSize {
		panic(fmt.Sprintf("count of short block: got %v, exp %v", len(block), encodedBlockHeaderSize))
	}
	// first byte is the block type
	tb, _, err := unpackBlock(block[1:])
	if err != nil {
		panic(fmt.Sprintf("BlockCount: error unpacking block: %s", err.Error()))
	}
	return CountTimestamps(tb)
}

// DecodeBlock takes a byte slice and decodes it into values of the appropriate type
// based on the block.
func DecodeBlock(block []byte, vals []Value) ([]Value, error) {
	if len(block) <= encodedBlockHeaderSize {
		panic(fmt.Sprintf("decode of short block: got %v, exp %v", len(block), encodedBlockHeaderSize))
	}

	blockType, err := BlockType(block)
	if err != nil {
		return nil, err
	}

	switch blockType {
	case BlockFloat64:
		var buf []FloatValue
		decoded, err := DecodeFloatBlock(block, &buf)
		if len(vals) < len(decoded) {
			vals = make([]Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = decoded[i]
		}
		return vals[:len(decoded)], err
	case BlockInteger:
		var buf []IntegerValue
		decoded, err := DecodeIntegerBlock(block, &buf)
		if len(vals) < len(decoded) {
			vals = make([]Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = decoded[i]
		}
		return vals[:len(decoded)], err

	case BlockBoolean:
		var buf []BooleanValue
		decoded, err := DecodeBooleanBlock(block, &buf)
		if len(vals) < len(decoded) {
			vals = make([]Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = decoded[i]
		}
		return vals[:len(decoded)], err

	case BlockString:
		var buf []StringValue
		decoded, err := DecodeStringBlock(block, &buf)
		if len(vals) < len(decoded) {
			vals = make([]Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = decoded[i]
		}
		return vals[:len(decoded)], err

	default:
		panic(fmt.Sprintf("unknown block type: %d", blockType))
	}
}

// FloatValue represents a float64 value.
type FloatValue struct {
	unixnano int64
	value    float64
}

// UnixNano returns the timestamp of the value.
func (f FloatValue) UnixNano() int64 {
	return f.unixnano
}

// Value returns the underlying float64 value.
func (f FloatValue) Value() interface{} {
	return f.value
}

// Size returns the number of bytes necessary to represent the value and its timestamp.
func (f FloatValue) Size() int {
	return 16
}

// String returns the string representation of the value and its timestamp.
func (f FloatValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, f.unixnano), f.value)
}

func encodeFloatBlock(buf []byte, values []Value) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// A float block is encoded using different compression strategies
	// for timestamps and values.

	// Encode values using Gorilla float compression
	venc := getFloatEncoder()

	// Encode timestamps using an adaptive encoder that uses delta-encoding,
	// frame-or-reference and run length encoding.
	tsenc := getTimeEncoder(len(values))

	var b []byte
	err := func() error {
		for _, v := range values {
			vv := v.(FloatValue)
			tsenc.Write(vv.unixnano)
			venc.Push(vv.value)
		}
		venc.Finish()

		// Encoded timestamp values
		tb, err := tsenc.Bytes()
		if err != nil {
			return err
		}
		// Encoded float values
		vb, err := venc.Bytes()
		if err != nil {
			return err
		}

		// Prepend the first timestamp of the block in the first 8 bytes and the block
		// in the next byte, followed by the block
		b = packBlock(buf, BlockFloat64, tb, vb)

		return nil
	}()

	putTimeEncoder(tsenc)
	putFloatEncoder(venc)

	return b, err
}

// DecodeFloatBlock decodes the float block from the byte slice
// and appends the float values to a.
func DecodeFloatBlock(block []byte, a *[]FloatValue) ([]FloatValue, error) {
	// Block type is the next block, make sure we actually have a float block
	blockType := block[0]
	if blockType != BlockFloat64 {
		return nil, fmt.Errorf("invalid block type: exp %d, got %d", BlockFloat64, blockType)
	}
	block = block[1:]

	tb, vb, err := unpackBlock(block)
	if err != nil {
		return nil, err
	}

	tdec := timeDecoderPool.Get(0).(*TimeDecoder)
	vdec := floatDecoderPool.Get(0).(*FloatDecoder)

	var i int
	err = func() error {
		// Setup our timestamp and value decoders
		tdec.Init(tb)
		err = vdec.SetBytes(vb)
		if err != nil {
			return err
		}

		// Decode both a timestamp and value
		for tdec.Next() && vdec.Next() {
			ts := tdec.Read()
			v := vdec.Values()
			if i < len(*a) {
				elem := &(*a)[i]
				elem.unixnano = ts
				elem.value = v
			} else {
				*a = append(*a, FloatValue{ts, v})
			}
			i++
		}

		// Did timestamp decoding have an error?
		err = tdec.Error()
		if err != nil {
			return err
		}

		// Did float decoding have an error?
		err = vdec.Error()
		if err != nil {
			return err
		}
		return nil
	}()

	timeDecoderPool.Put(tdec)
	floatDecoderPool.Put(vdec)

	return (*a)[:i], err
}

// BooleanValue represents a boolean value.
type BooleanValue struct {
	unixnano int64
	value    bool
}

// Size returns the number of bytes necessary to represent the value and its timestamp.
func (b BooleanValue) Size() int {
	return 9
}

// UnixNano returns the timestamp of the value in nanoseconds since unix epoch.
func (b BooleanValue) UnixNano() int64 {
	return b.unixnano
}

// Value returns the underlying boolean value.
func (b BooleanValue) Value() interface{} {
	return b.value
}

// String returns the string representation of the value and its timestamp.
func (f BooleanValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, f.unixnano), f.Value())
}

func encodeBooleanBlock(buf []byte, values []Value) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// A boolean block is encoded using different compression strategies
	// for timestamps and values.
	venc := getBooleanEncoder(len(values))

	// Encode timestamps using an adaptive encoder
	tsenc := getTimeEncoder(len(values))

	var b []byte
	err := func() error {
		for _, v := range values {
			vv := v.(BooleanValue)
			tsenc.Write(vv.unixnano)
			venc.Write(vv.value)
		}

		// Encoded timestamp values
		tb, err := tsenc.Bytes()
		if err != nil {
			return err
		}
		// Encoded float values
		vb, err := venc.Bytes()
		if err != nil {
			return err
		}

		// Prepend the first timestamp of the block in the first 8 bytes and the block
		// in the next byte, followed by the block
		b = packBlock(buf, BlockBoolean, tb, vb)
		return nil
	}()

	putTimeEncoder(tsenc)
	putBooleanEncoder(venc)

	return b, err
}

// DecodeBooleanBlock decodes the boolean block from the byte slice
// and appends the boolean values to a.
func DecodeBooleanBlock(block []byte, a *[]BooleanValue) ([]BooleanValue, error) {
	// Block type is the next block, make sure we actually have a float block
	blockType := block[0]
	if blockType != BlockBoolean {
		return nil, fmt.Errorf("invalid block type: exp %d, got %d", BlockBoolean, blockType)
	}
	block = block[1:]

	tb, vb, err := unpackBlock(block)
	if err != nil {
		return nil, err
	}

	tdec := timeDecoderPool.Get(0).(*TimeDecoder)
	vdec := booleanDecoderPool.Get(0).(*BooleanDecoder)

	var i int
	err = func() error {
		// Setup our timestamp and value decoders
		tdec.Init(tb)
		vdec.SetBytes(vb)

		// Decode both a timestamp and value
		for tdec.Next() && vdec.Next() {
			ts := tdec.Read()
			v := vdec.Read()
			if i < len(*a) {
				elem := &(*a)[i]
				elem.unixnano = ts
				elem.value = v
			} else {
				*a = append(*a, BooleanValue{ts, v})
			}
			i++
		}

		// Did timestamp decoding have an error?
		err = tdec.Error()
		if err != nil {
			return err
		}
		// Did boolean decoding have an error?
		err = vdec.Error()
		if err != nil {
			return err
		}
		return nil
	}()

	timeDecoderPool.Put(tdec)
	booleanDecoderPool.Put(vdec)

	return (*a)[:i], err
}

// FloatValue represents an int64 value.
type IntegerValue struct {
	unixnano int64
	value    int64
}

// Value returns the underlying int64 value.
func (v IntegerValue) Value() interface{} {
	return v.value
}

// UnixNano returns the timestamp of the value.
func (v IntegerValue) UnixNano() int64 {
	return v.unixnano
}

// Size returns the number of bytes necessary to represent the value and its timestamp.
func (v IntegerValue) Size() int {
	return 16
}

// String returns the string representation of the value and its timestamp.
func (f IntegerValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, f.unixnano), f.Value())
}

func encodeIntegerBlock(buf []byte, values []Value) ([]byte, error) {
	tsEnc := getTimeEncoder(len(values))
	vEnc := getIntegerEncoder(len(values))

	var b []byte
	err := func() error {
		for _, v := range values {
			vv := v.(IntegerValue)
			tsEnc.Write(vv.unixnano)
			vEnc.Write(vv.value)
		}

		// Encoded timestamp values
		tb, err := tsEnc.Bytes()
		if err != nil {
			return err
		}
		// Encoded int64 values
		vb, err := vEnc.Bytes()
		if err != nil {
			return err
		}

		// Prepend the first timestamp of the block in the first 8 bytes
		b = packBlock(buf, BlockInteger, tb, vb)
		return nil
	}()

	putTimeEncoder(tsEnc)
	putIntegerEncoder(vEnc)

	return b, err
}

// DecodeIntegerBlock decodes the integer block from the byte slice
// and appends the integer values to a.
func DecodeIntegerBlock(block []byte, a *[]IntegerValue) ([]IntegerValue, error) {
	blockType := block[0]
	if blockType != BlockInteger {
		return nil, fmt.Errorf("invalid block type: exp %d, got %d", BlockInteger, blockType)
	}

	block = block[1:]

	// The first 8 bytes is the minimum timestamp of the block
	tb, vb, err := unpackBlock(block)
	if err != nil {
		return nil, err
	}

	tdec := timeDecoderPool.Get(0).(*TimeDecoder)
	vdec := integerDecoderPool.Get(0).(*IntegerDecoder)

	var i int
	err = func() error {
		// Setup our timestamp and value decoders
		tdec.Init(tb)
		vdec.SetBytes(vb)

		// Decode both a timestamp and value
		for tdec.Next() && vdec.Next() {
			ts := tdec.Read()
			v := vdec.Read()
			if i < len(*a) {
				elem := &(*a)[i]
				elem.unixnano = ts
				elem.value = v
			} else {
				*a = append(*a, IntegerValue{ts, v})
			}
			i++
		}

		// Did timestamp decoding have an error?
		err = tdec.Error()
		if err != nil {
			return err
		}
		// Did int64 decoding have an error?
		err = vdec.Error()
		if err != nil {
			return err
		}
		return nil
	}()

	timeDecoderPool.Put(tdec)
	integerDecoderPool.Put(vdec)

	return (*a)[:i], err
}

// StringValue represents a string value.
type StringValue struct {
	unixnano int64
	value    string
}

// Value returns the underlying string value.
func (v StringValue) Value() interface{} {
	return v.value
}

// UnixNano returns the timestamp of the value.
func (v StringValue) UnixNano() int64 {
	return v.unixnano
}

// Size returns the number of bytes necessary to represent the value and its timestamp.
func (v StringValue) Size() int {
	return 8 + len(v.value)
}

// String returns the string representation of the value and its timestamp.
func (f StringValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, f.unixnano), f.Value())
}

func encodeStringBlock(buf []byte, values []Value) ([]byte, error) {
	tsEnc := getTimeEncoder(len(values))
	vEnc := getStringEncoder(len(values) * len(values[0].(StringValue).value))

	var b []byte
	err := func() error {
		for _, v := range values {
			vv := v.(StringValue)
			tsEnc.Write(vv.unixnano)
			vEnc.Write(vv.value)
		}

		// Encoded timestamp values
		tb, err := tsEnc.Bytes()
		if err != nil {
			return err
		}
		// Encoded string values
		vb, err := vEnc.Bytes()
		if err != nil {
			return err
		}

		// Prepend the first timestamp of the block in the first 8 bytes
		b = packBlock(buf, BlockString, tb, vb)

		return nil
	}()

	putTimeEncoder(tsEnc)
	putStringEncoder(vEnc)

	return b, err
}

// DecodeStringBlock decodes the string block from the byte slice
// and appends the string values to a.
func DecodeStringBlock(block []byte, a *[]StringValue) ([]StringValue, error) {
	blockType := block[0]
	if blockType != BlockString {
		return nil, fmt.Errorf("invalid block type: exp %d, got %d", BlockString, blockType)
	}

	block = block[1:]

	// The first 8 bytes is the minimum timestamp of the block
	tb, vb, err := unpackBlock(block)
	if err != nil {
		return nil, err
	}

	tdec := timeDecoderPool.Get(0).(*TimeDecoder)
	vdec := stringDecoderPool.Get(0).(*StringDecoder)

	var i int
	err = func() error {
		// Setup our timestamp and value decoders
		tdec.Init(tb)
		err = vdec.SetBytes(vb)
		if err != nil {
			return err
		}

		// Decode both a timestamp and value
		for tdec.Next() && vdec.Next() {
			ts := tdec.Read()
			v := vdec.Read()
			if i < len(*a) {
				elem := &(*a)[i]
				elem.unixnano = ts
				elem.value = v
			} else {
				*a = append(*a, StringValue{ts, v})
			}
			i++
		}

		// Did timestamp decoding have an error?
		err = tdec.Error()
		if err != nil {
			return err
		}
		// Did string decoding have an error?
		err = vdec.Error()
		if err != nil {
			return err
		}
		return nil
	}()

	timeDecoderPool.Put(tdec)
	stringDecoderPool.Put(vdec)

	return (*a)[:i], err
}

func packBlock(buf []byte, typ byte, ts []byte, values []byte) []byte {
	// We encode the length of the timestamp block using a variable byte encoding.
	// This allows small byte slices to take up 1 byte while larger ones use 2 or more.
	sz := 1 + binary.MaxVarintLen64 + len(ts) + len(values)
	if cap(buf) < sz {
		buf = make([]byte, sz)
	}
	b := buf[:sz]
	b[0] = typ
	i := binary.PutUvarint(b[1:1+binary.MaxVarintLen64], uint64(len(ts)))
	i += 1

	// block is <len timestamp bytes>, <ts bytes>, <value bytes>
	copy(b[i:], ts)
	// We don't encode the value length because we know it's the rest of the block after
	// the timestamp block.
	copy(b[i+len(ts):], values)
	return b[:i+len(ts)+len(values)]
}

func unpackBlock(buf []byte) (ts, values []byte, err error) {
	// Unpack the timestamp block length
	tsLen, i := binary.Uvarint(buf)
	if i <= 0 {
		err = fmt.Errorf("unpackBlock: unable to read timestamp block length")
		return
	}

	// Unpack the timestamp bytes
	tsIdx := int(i) + int(tsLen)
	if tsIdx > len(buf) {
		err = fmt.Errorf("unpackBlock: not enough data for timestamp")
		return
	}
	ts = buf[int(i):tsIdx]

	// Unpack the value bytes
	values = buf[tsIdx:]
	return
}

// ZigZagEncode converts a int64 to a uint64 by zig zagging negative and positive values
// across even and odd numbers.  Eg. [0,-1,1,-2] becomes [0, 1, 2, 3].
func ZigZagEncode(x int64) uint64 {
	return uint64(uint64(x<<1) ^ uint64((int64(x) >> 63)))
}

// ZigZagDecode converts a previously zigzag encoded uint64 back to a int64.
func ZigZagDecode(v uint64) int64 {
	return int64((v >> 1) ^ uint64((int64(v&1)<<63)>>63))
}
func getTimeEncoder(sz int) TimeEncoder {
	x := timeEncoderPool.Get(sz).(TimeEncoder)
	x.Reset()
	return x
}
func putTimeEncoder(enc TimeEncoder) { timeEncoderPool.Put(enc) }

func getIntegerEncoder(sz int) IntegerEncoder {
	x := integerEncoderPool.Get(sz).(IntegerEncoder)
	x.Reset()
	return x
}
func putIntegerEncoder(enc IntegerEncoder) { integerEncoderPool.Put(enc) }

func getFloatEncoder() *FloatEncoder {
	x := floatEncoderPool.Get(1024).(*FloatEncoder)
	x.Reset()
	return x
}
func putFloatEncoder(enc *FloatEncoder) { floatEncoderPool.Put(enc) }

func getStringEncoder(sz int) StringEncoder {
	x := stringEncoderPool.Get(sz).(StringEncoder)
	x.Reset()
	return x
}
func putStringEncoder(enc StringEncoder) { stringEncoderPool.Put(enc) }

func getBooleanEncoder(sz int) BooleanEncoder {
	x := booleanEncoderPool.Get(sz).(BooleanEncoder)
	x.Reset()
	return x
}
func putBooleanEncoder(enc BooleanEncoder) { booleanEncoderPool.Put(enc) }
