package pipeline

import (
	"fmt"
	"time"

	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/tick"
	"github.com/influxdata/kapacitor/udf"
)

// A UDFNode is a User Defined Function.
// UDFs can be defined in the configuration file. in the [udf] section.
//
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
	udf.describer = tick.NewReflectionDescriber(udf)
	parent.linkChild(udf)
	return udf
}

func (u *UDFNode) Desc() string {
	return u.desc
}

func (u *UDFNode) HasMethod(name string) bool {
	_, ok := u.options[name]
	if ok {
		return ok
	}
	return u.describer.HasMethod(name)
}

func (u *UDFNode) CallMethod(name string, args ...interface{}) (interface{}, error) {
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
	return u.describer.CallMethod(name, args...)
}

func (u *UDFNode) HasProperty(name string) bool {
	return u.describer.HasProperty(name)
}

func (u *UDFNode) Property(name string) interface{} {
	return u.describer.Property(name)
}

func (u *UDFNode) SetProperty(name string, value interface{}) error {
	return u.describer.SetProperty(name, value)
}
