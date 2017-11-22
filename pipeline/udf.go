package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor/tick"
	"github.com/influxdata/kapacitor/udf/agent"
)

// A UDFNode is a node that can run a User Defined Function (UDF) in a separate process.
//
// A UDF is a custom script or binary that can communicate via Kapacitor's UDF RPC protocol.
// The path and arguments to the UDF program are specified in Kapacitor's configuration.
// Using TICKscripts you can invoke and configure your UDF for each task.
//
// See the [README.md](https://github.com/influxdata/kapacitor/tree/master/udf/agent/)
// for details on how to write your own UDF.
//
// UDFs are configured via Kapacitor's main configuration file.
//
// Example:
//    [udf]
//    [udf.functions]
//        # Example moving average UDF.
//        [udf.functions.movingAverage]
//            prog = "/path/to/executable/moving_avg"
//            args = []
//            timeout = "10s"
//
// UDFs are first class objects in TICKscripts and are referenced via their configuration name.
//
// Example:
//     // Given you have a UDF that computes a moving average
//     // The UDF can define what its options are and then can be
//     // invoked via a TICKscript like so:
//     stream
//         |from()...
//         @movingAverage()
//             .field('value')
//             .size(100)
//             .as('mavg')
//         |httpOut('movingaverage')
//
// NOTE: The UDF process runs as the same user as the Kapacitor daemon.
// As a result, make sure the user is properly secured, as well as the configuration file.
type UDFNode struct {
	chainnode

	UDFName string
	options map[string]*agent.OptionInfo

	// Options that were set on the node
	// tick:ignore
	Options []*agent.Option

	describer *tick.ReflectionDescriber
}

func NewUDF(
	parent Node,
	name string,
	wants,
	provides agent.EdgeType,
	options map[string]*agent.OptionInfo,
) *UDFNode {
	var pwants, pprovides EdgeType
	switch wants {
	case agent.EdgeType_STREAM:
		pwants = StreamEdge
	case agent.EdgeType_BATCH:
		pwants = BatchEdge
	}
	switch provides {
	case agent.EdgeType_STREAM:
		pprovides = StreamEdge
	case agent.EdgeType_BATCH:
		pprovides = BatchEdge
	}
	udf := &UDFNode{
		chainnode: newBasicChainNode(name, pwants, pprovides),
		UDFName:   name,
		options:   options,
	}
	udf.describer, _ = tick.NewReflectionDescriber(udf, nil)
	parent.linkChild(udf)
	return udf
}

// tick:ignore
func (u *UDFNode) Desc() string {
	return u.UDFName
}

// tick:ignore
func (u *UDFNode) HasChainMethod(name string) bool {
	return u.describer.HasChainMethod(name)
}

// tick:ignore
func (u *UDFNode) CallChainMethod(name string, args ...interface{}) (interface{}, error) {
	return u.describer.CallChainMethod(name, args...)
}

// tick:ignore
func (u *UDFNode) HasProperty(name string) bool {
	_, ok := u.options[name]
	if ok {
		return ok
	}
	return u.describer.HasProperty(name)
}

// tick:ignore
func (u *UDFNode) Property(name string) interface{} {
	return u.describer.Property(name)
}

// tick:ignore
func (u *UDFNode) SetProperty(name string, args ...interface{}) (interface{}, error) {
	opt, ok := u.options[name]
	if ok {
		if got, exp := len(args), len(opt.ValueTypes); got != exp {
			return nil, fmt.Errorf("unexpected number of args to %s, got %d expected %d", name, got, exp)
		}
		values := make([]*agent.OptionValue, len(args))
		for i, arg := range args {
			values[i] = &agent.OptionValue{}
			switch v := arg.(type) {
			case bool:
				values[i].Type = agent.ValueType_BOOL
				values[i].Value = &agent.OptionValue_BoolValue{v}
			case int64:
				values[i].Type = agent.ValueType_INT
				values[i].Value = &agent.OptionValue_IntValue{v}
			case float64:
				values[i].Type = agent.ValueType_DOUBLE
				values[i].Value = &agent.OptionValue_DoubleValue{v}
			case string:
				values[i].Type = agent.ValueType_STRING
				values[i].Value = &agent.OptionValue_StringValue{v}
			case time.Duration:
				values[i].Type = agent.ValueType_DURATION
				values[i].Value = &agent.OptionValue_DurationValue{int64(v)}
			}
			if values[i].Type != opt.ValueTypes[i] {
				return nil, fmt.Errorf("unexpected arg to %s, got %v expected %v", name, values[i].Type, opt.ValueTypes[i])
			}
		}
		u.Options = append(u.Options, &agent.Option{
			Name:   name,
			Values: values,
		})
		return u, nil
	}
	return u.describer.SetProperty(name, args...)
}

// MarshalJSON converts UDFNode to JSON
// tick:ignore
func (u *UDFNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		SetType("udf").
		SetID(u.ID()).
		Set("udfName", u.UDFName)
	for _, o := range u.Options {
		args := []interface{}{}
		for _, v := range o.Values {
			switch v.Type {
			case agent.ValueType_BOOL:
				args = append(args, v.GetBoolValue())
			case agent.ValueType_INT:
				args = append(args, v.GetIntValue())
			case agent.ValueType_DOUBLE:
				args = append(args, v.GetDoubleValue())
			case agent.ValueType_STRING:
				args = append(args, v.GetStringValue())
			case agent.ValueType_DURATION:
				dur := influxql.FormatDuration(time.Duration(v.GetDurationValue()))
				args = append(args, dur)
			}
		}
		props = props.Set(o.Name, args)
	}
	return json.Marshal(&props)
}

func (u *UDFNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("udf")
	if err != nil {
		return err
	}

	if u.id, err = props.ID(); err != nil {
		return err
	}

	if u.UDFName, err = props.String("udfName"); err != nil {
		return err
	}

	properties := map[string][]*agent.OptionValue{}
	for name, v := range props {
		if name == NodeID || name == NodeTypeOf || name == "udfName" {
			continue
		}
		args, ok := v.([]interface{})
		if !ok {
			return fmt.Errorf("property %s is not a list of values but is %T", name, v)
		}
		values := make([]*agent.OptionValue, len(args))
		for i, arg := range args {
			switch v := arg.(type) {
			case bool:
				values[i] = &agent.OptionValue{
					Type:  agent.ValueType_BOOL,
					Value: &agent.OptionValue_BoolValue{v},
				}
			case json.Number:
				integer, err := v.Int64()
				if err == nil {
					values[i] = &agent.OptionValue{
						Type:  agent.ValueType_INT,
						Value: &agent.OptionValue_IntValue{integer},
					}
					break
				}

				flt, err := v.Float64()
				if err != nil {
					return err
				}
				values[i] = &agent.OptionValue{
					Type:  agent.ValueType_DOUBLE,
					Value: &agent.OptionValue_DoubleValue{flt},
				}
			case string:
				dur, err := influxql.ParseDuration(v)
				if err == nil {
					values[i] = &agent.OptionValue{
						Type:  agent.ValueType_DURATION,
						Value: &agent.OptionValue_DurationValue{int64(dur)},
					}
					break
				}
				values[i] = &agent.OptionValue{
					Type:  agent.ValueType_STRING,
					Value: &agent.OptionValue_StringValue{v},
				}
			}
		}
		properties[name] = values
	}
	var names []string
	for name := range properties {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		u.Options = append(u.Options, &agent.Option{
			Name:   name,
			Values: properties[name],
		})
	}
	return nil
}

// UnmarshalJSON converts JSON to UDFNode
// tick:ignore
func (u *UDFNode) UnmarshalJSON(data []byte) error {
	props, err := NewJSONNode(data)
	if err != nil {
		return err
	}
	return u.unmarshal(props)
}

// JSONNode contains all fields associated with a node.  `typeOf`
// is used to determine which type of node this is.
// JSONNode is used by UDFNode specifically for marshaling and unmarshaling
// the UDF to json
type JSONNode map[string]interface{}

// NewJSONNode decodes JSON bytes into a JSONNode
func NewJSONNode(data []byte) (JSONNode, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var input JSONNode
	err := dec.Decode(&input)
	return input, err
}

// CheckTypeOf tests that the typeOf field is correctly set to typ.
func (j JSONNode) CheckTypeOf(typ string) error {
	t, ok := j[NodeTypeOf]
	if !ok {
		return fmt.Errorf("missing typeOf field")
	}

	if t != typ {
		return fmt.Errorf("error unmarshaling node type %s; received %s", typ, t)
	}
	return nil
}

// SetType adds the Node type information
func (j JSONNode) SetType(typ string) JSONNode {
	j[NodeTypeOf] = typ
	return j
}

// SetID adds the Node ID information
func (j JSONNode) SetID(id ID) JSONNode {
	j[NodeID] = fmt.Sprintf("%d", id)
	return j
}

// Set adds the key/value to the JSONNode
func (j JSONNode) Set(key string, value interface{}) JSONNode {
	j[key] = value
	return j
}

// Field returns expected field or error if field doesn't exist
func (j JSONNode) Field(field string) (interface{}, error) {
	fld, ok := j[field]
	if !ok {
		return nil, fmt.Errorf("missing expected field %s", field)
	}
	return fld, nil
}

// String reads the field for a string value
func (j JSONNode) String(field string) (string, error) {
	s, err := j.Field(field)
	if err != nil {
		return "", err
	}

	str, ok := s.(string)
	if !ok {
		return "", fmt.Errorf("field %s is not a string value but is %T", field, s)
	}
	return str, nil
}

// ID returns the unique ID for this node.  This ID is used
// as the id of the parent and children in the Edges structure.
func (j JSONNode) ID() (ID, error) {
	i, err := j.String(NodeID)
	if err != nil {
		return 0, err
	}
	id, err := strconv.Atoi(i)
	return ID(id), err
}
