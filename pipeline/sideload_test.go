package pipeline

import (
	"testing"
)

func TestSideloadNode_MarshalJSON(t *testing.T) {
	type fields struct {
		Source string
		Order  []string
		Fields map[string]interface{}
		Tags   map[string]string
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
	}{
		{
			name: "all fields set",
			fields: fields{
				Source: "file:///src",
				Order:  []string{"a", "b", "c"},
				Fields: map[string]interface{}{
					"f1": 42.0,
					"f2": "",
				},
				Tags: map[string]string{
					"t1": "k1",
					"t2": "",
				},
			},
			want: `{
    "typeOf": "sideload",
    "id": "0",
    "source": "file:///src",
    "order": [
        "a",
        "b",
        "c"
    ],
    "fields": {
        "f1": 42,
        "f2": ""
    },
    "tags": {
        "t1": "k1",
        "t2": ""
    }
}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newSideloadNode(StreamEdge)
			w.setID(0)
			w.Source = tt.fields.Source
			w.OrderList = tt.fields.Order
			w.Fields = tt.fields.Fields
			w.Tags = tt.fields.Tags
			MarshalIndentTestHelper(t, w, tt.wantErr, tt.want)
		})
	}
}

func TestSideloadNode_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *SideloadNode
		wantErr bool
	}{
		{
			name: "all fields set",
			input: `{
    "typeOf": "sideload",
    "id": "0",
    "source": "file:///src",
    "order": ["a", "b", "c"],
    "fields": {
        "f1": 42.0,
        "f2": ""
    },
    "tags": {
        "t1": "k1",
        "t2": ""
    }
}`,
			want: &SideloadNode{
				Source:    "file:///src",
				OrderList: []string{"a", "b", "c"},
				Fields: map[string]interface{}{
					"f1": 42.0,
					"f2": "",
				},
				Tags: map[string]string{
					"t1": "k1",
					"t2": "",
				},
			},
		},
		{
			name:  "set id correctly",
			input: `{"typeOf":"sideload","id":"5"}`,
			want: &SideloadNode{
				chainnode: chainnode{
					node: node{
						id: 5,
					},
				},
			},
		},
		{
			name:    "invalid data",
			input:   `{"typeOf":"sideload","id":"0", "source": 56.0}`,
			wantErr: true,
		},
		{
			name:    "invalid node type",
			input:   `{"typeOf":"invalid","id"0"}`,
			wantErr: true,
		},
		{
			name:    "invalid id type",
			input:   `{"typeOf":"window","id":"invalid"}`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &SideloadNode{}
			UnmarshalJSONTestHelper(t, []byte(tt.input), w, tt.wantErr, tt.want)
		})
	}

}
