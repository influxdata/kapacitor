package pipeline

import (
	"fmt"
	"time"

	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/tick"
	"github.com/influxdata/kapacitor/udf"
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
// As a result make the user is properly secured as well as the configuration file.
type UDFNode struct {
	chainnode

	desc string
	//tick:ignore
	Commander command.Commander
	// tick:ignore
	Timeout time.Duration

	options map[string]*udf.OptionInfo

	// Options that were set on the node
	// tick:ignore
	Options []*udf.Option

	describer *tick.ReflectionDescriber
}

func NewUDF(
	parent Node,
	name string,
	commander command.Commander,
	timeout time.Duration,
	wants,
	provides EdgeType,
	options map[string]*udf.OptionInfo,
) *UDFNode {
	udf := &UDFNode{
		chainnode: newBasicChainNode(name, wants, provides),
		desc:      name,
		Commander: commander,
		Timeout:   timeout,
		options:   options,
	}
	udf.describer, _ = tick.NewReflectionDescriber(udf)
	parent.linkChild(udf)
	return udf
}

// tick:ignore
func (u *UDFNode) Desc() string {
	return u.desc
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
		values := make([]*udf.OptionValue, len(args))
		for i, arg := range args {
			values[i] = &udf.OptionValue{}
			switch v := arg.(type) {
			case bool:
				values[i].Type = udf.ValueType_BOOL
				values[i].Value = &udf.OptionValue_BoolValue{v}
			case int64:
				values[i].Type = udf.ValueType_INT
				values[i].Value = &udf.OptionValue_IntValue{v}
			case float64:
				values[i].Type = udf.ValueType_DOUBLE
				values[i].Value = &udf.OptionValue_DoubleValue{v}
			case string:
				values[i].Type = udf.ValueType_STRING
				values[i].Value = &udf.OptionValue_StringValue{v}
			case time.Duration:
				values[i].Type = udf.ValueType_DURATION
				values[i].Value = &udf.OptionValue_DurationValue{int64(v)}
			}
			if values[i].Type != opt.ValueTypes[i] {
				return nil, fmt.Errorf("unexpected arg to %s, got %v expected %v", name, values[i].Type, opt.ValueTypes[i])
			}
		}
		u.Options = append(u.Options, &udf.Option{
			Name:   name,
			Values: values,
		})
		return u, nil
	}
	return u.describer.SetProperty(name, args...)
}
