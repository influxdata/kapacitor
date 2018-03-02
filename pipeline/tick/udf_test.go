package tick_test

import (
	"testing"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/udf/agent"
)

func TestUDF(t *testing.T) {
	pipe, _, from := StreamFrom()
	options := map[string]*agent.OptionInfo{
		"mph": &agent.OptionInfo{
			ValueTypes: []agent.ValueType{
				agent.ValueType_INT,
			},
		},
		"gigawatts": &agent.OptionInfo{
			ValueTypes: []agent.ValueType{
				agent.ValueType_DOUBLE,
			},
		},
		"nearClockTower": &agent.OptionInfo{
			ValueTypes: []agent.ValueType{
				agent.ValueType_BOOL,
			},
		},
		"martySays": &agent.OptionInfo{
			ValueTypes: []agent.ValueType{
				agent.ValueType_STRING,
			},
		},
		"future": &agent.OptionInfo{
			ValueTypes: []agent.ValueType{
				agent.ValueType_DURATION,
				agent.ValueType_STRING,
			},
		},
	}
	udf := pipeline.NewUDF(from, "delorean", agent.EdgeType_STREAM, agent.EdgeType_STREAM, options)
	udf.Options = []*agent.Option{
		{
			Name: "mph",
			Values: []*agent.OptionValue{
				{
					Type: agent.ValueType_INT,
					Value: &agent.OptionValue_IntValue{
						IntValue: 88,
					},
				},
			},
		},
		{
			Name: "gigawatts",
			Values: []*agent.OptionValue{
				{
					Type: agent.ValueType_DOUBLE,
					Value: &agent.OptionValue_DoubleValue{
						DoubleValue: 1.21,
					},
				},
			},
		},
		{
			Name: "nearClockTower",
			Values: []*agent.OptionValue{
				{
					Type: agent.ValueType_BOOL,
					Value: &agent.OptionValue_BoolValue{
						BoolValue: true,
					},
				},
			},
		},
		{
			Name: "martySays",
			Values: []*agent.OptionValue{
				{
					Type: agent.ValueType_STRING,
					Value: &agent.OptionValue_StringValue{
						StringValue: "Doc!",
					},
				},
			},
		},
		{
			Name: "future",
			Values: []*agent.OptionValue{
				{
					Type: agent.ValueType_DURATION,
					Value: &agent.OptionValue_DurationValue{
						DurationValue: 946708560000000000, // 30 years!
					},
				},
				{
					Type: agent.ValueType_STRING,
					Value: &agent.OptionValue_StringValue{
						StringValue: "years",
					},
				},
			},
		},
	}

	want := `stream
    |from()
    @delorean()
        .mph(88)
        .gigawatts(1.21)
        .nearClockTower(TRUE)
        .martySays('Doc!')
        .future(15778476m, 'years')
`
	PipelineTickTestHelper(t, pipe, want, udf)
}
