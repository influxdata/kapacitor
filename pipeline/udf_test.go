package pipeline

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/influxdata/kapacitor/udf/agent"
)

func TestUDFNode_MarshalJSON(t *testing.T) {
	type fields struct {
		Options []*agent.Option
		UDFName string
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
	}{
		{
			name: "all udf field types",
			fields: fields{
				Options: []*agent.Option{
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
				},
				UDFName: "delorean",
			},
			want: `{
    "future": [
        "15778476m",
        "years"
    ],
    "gigawatts": [
        1.21
    ],
    "id": "0",
    "martySays": [
        "Doc!"
    ],
    "mph": [
        88
    ],
    "nearClockTower": [
        true
    ],
    "typeOf": "udf",
    "udfName": "delorean"
}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := &UDFNode{
				Options: tt.fields.Options,
				UDFName: tt.fields.UDFName,
			}
			MarshalIndentTestHelper(t, u, tt.wantErr, tt.want)
		})
	}
}

func TestUDFNode_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *UDFNode
		wantErr bool
	}{
		{
			name: "every type of option set",
			input: `{
    "future": [
        "15778476m",
        "years"
    ],
    "gigawatts": [
        1.21
    ],
    "id": "0",
    "martySays": [
        "Doc!"
    ],
    "mph": [
        88
    ],
    "nearClockTower": [
        true
    ],
    "typeOf": "udf",
    "udfName": "delorean"
}`,
			want: &UDFNode{
				options: map[string]*agent.OptionInfo{
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
				},
				Options: []*agent.Option{
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
				},
				UDFName: "delorean",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := &UDFNode{
				options: map[string]*agent.OptionInfo{
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
				},
			}
			err := json.Unmarshal([]byte(tt.input), u)
			if (err != nil) != tt.wantErr {
				t.Errorf("UDFNode.UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			u.options = nil
			tt.want.options = nil
			if !reflect.DeepEqual(u, tt.want) {
				t.Errorf("UDFNode.UnmarshalJSON() =\n%#+v\nwant\n%#+v", u, tt.want)
			}
		})
	}

}
