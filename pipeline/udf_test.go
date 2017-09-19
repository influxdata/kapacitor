package pipeline

import (
	"bytes"
	"testing"

	"github.com/influxdata/kapacitor/udf/agent"
)

func TestUDFNode_Tick(t *testing.T) {
	type args struct {
		name    string
		options []*agent.Option
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "udf with integer option",
			args: args{
				name: "deloran",
				options: []*agent.Option{
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
				},
			},
			want: `@deloran().mph(88)`,
		},
		{
			name: "udf with multiple integer arguments",
			args: args{
				name: "deloran",
				options: []*agent.Option{
					{
						Name: "year",
						Values: []*agent.OptionValue{
							{
								Type: agent.ValueType_INT,
								Value: &agent.OptionValue_IntValue{
									IntValue: 1985,
								},
							},
							{
								Type: agent.ValueType_INT,
								Value: &agent.OptionValue_IntValue{
									IntValue: 1955,
								},
							},
						},
					},
				},
			},
			want: `@deloran().year(1985, 1955)`,
		},
		{
			name: "udf with integer, double, bool, string, duration",
			args: args{
				name: "deloran",
				options: []*agent.Option{
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
						},
					},
				},
			},
			want: `@deloran().mph(88).gigawatts(1.21).nearClockTower(TRUE).martySays('Doc!').future(15778476m)`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			n := &UDFNode{
				UDFName: tt.args.name,
				Options: tt.args.options,
			}
			var buf bytes.Buffer
			n.Tick(&buf)
			got := buf.String()
			if got != tt.want {
				t.Errorf("%q. TestUDFNode_Tick() =\n%v\n want\n%v\n", tt.name, got, tt.want)
			}
		})
	}
}
