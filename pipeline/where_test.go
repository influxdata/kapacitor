package pipeline

import (
	"encoding/json"
	"reflect"
	"regexp"
	"testing"

	"github.com/influxdata/kapacitor/tick/ast"
)

func TestWhereNode_MarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		lambda  *ast.LambdaNode
		id      ID
		want    string
		wantErr bool
	}{
		{
			name: "all fields set",
			lambda: &ast.LambdaNode{
				Expression: &ast.BinaryNode{
					Operator: ast.TokenAnd,
					Left: &ast.LambdaNode{
						Expression: &ast.BinaryNode{
							Left: &ast.ReferenceNode{
								Reference: "cpu",
							},
							Right: &ast.StringNode{
								Literal: "cpu-total",
							},
							Operator: ast.TokenNotEqual,
						},
					},
					Right: &ast.LambdaNode{
						Expression: &ast.BinaryNode{
							Left: &ast.ReferenceNode{
								Reference: "host",
							},
							Right: &ast.RegexNode{
								Literal: `logger\d+`,
							},
							Operator: ast.TokenRegexEqual,
						},
					},
				},
			},
			want: `{
    "typeOf": "where",
    "id": "0",
    "lambda": {
        "expression": {
            "left": {
                "expression": {
                    "left": {
                        "reference": "cpu",
                        "typeOf": "reference"
                    },
                    "operator": "!=",
                    "right": {
                        "literal": "cpu-total",
                        "typeOf": "string"
                    },
                    "typeOf": "binary"
                },
                "typeOf": "lambda"
            },
            "operator": "AND",
            "right": {
                "expression": {
                    "left": {
                        "reference": "host",
                        "typeOf": "reference"
                    },
                    "operator": "=~",
                    "right": {
                        "regex": "logger\\d+",
                        "typeOf": "regex"
                    },
                    "typeOf": "binary"
                },
                "typeOf": "lambda"
            },
            "typeOf": "binary"
        },
        "typeOf": "lambda"
    }
}`,
		},
		{
			name: "different id",
			id:   5,
			want: `{
    "typeOf": "where",
    "id": "5",
    "lambda": null
}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newWhereNode(StreamEdge, tt.lambda)
			w.setID(tt.id)
			MarshalIndentTestHelper(t, w, tt.wantErr, tt.want)
		})
	}
}

func TestWhereNode_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *WhereNode
		wantErr bool
	}{
		{
			name: "all fields set",
			input: `{
    "typeOf": "where",
    "id": "0",
    "lambda": {
        "expression": {
            "left": {
                "expression": {
                    "left": {
                        "reference": "cpu",
                        "typeOf": "reference"
                    },
                    "operator": "!=",
                    "right": {
                        "literal": "cpu-total",
                        "typeOf": "string"
                    },
                    "typeOf": "binary"
                },
                "typeOf": "lambda"
            },
            "operator": "AND",
            "right": {
                "expression": {
                    "left": {
                        "reference": "host",
                        "typeOf": "reference"
                    },
                    "operator": "=~",
                    "right": {
                        "regex": "logger\\d+",
                        "typeOf": "regex"
                    },
                    "typeOf": "binary"
                },
                "typeOf": "lambda"
            },
            "typeOf": "binary"
        },
        "typeOf": "lambda"
    }
}`,
			want: &WhereNode{
				Lambda: &ast.LambdaNode{
					Expression: &ast.BinaryNode{
						Operator: ast.TokenAnd,
						Left: &ast.LambdaNode{
							Expression: &ast.BinaryNode{
								Left: &ast.ReferenceNode{
									Reference: "cpu",
								},
								Right: &ast.StringNode{
									Literal: "cpu-total",
								},
								Operator: ast.TokenNotEqual,
							},
						},
						Right: &ast.LambdaNode{
							Expression: &ast.BinaryNode{
								Left: &ast.ReferenceNode{
									Reference: "host",
								},
								Right: &ast.RegexNode{
									Regex: func() *regexp.Regexp { r, _ := regexp.Compile(`logger\d+`); return r }(),
								},
								Operator: ast.TokenRegexEqual,
							},
						},
					},
				},
			},
		},
		{
			name:  "set id correctly",
			input: `{"typeOf":"where","id":"5", "lambda": null}`,
			want: &WhereNode{
				chainnode: chainnode{
					node: node{
						id: 5,
					},
				},
			},
		},
		{
			name:    "invalid data",
			input:   `{"typeOf":"where","id":"0", "lambda": "invalid"}`,
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
			w := &WhereNode{}
			err := json.Unmarshal([]byte(tt.input), w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WhereNode.UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(w, tt.want) {
				t.Errorf("WhereNode.UnmarshalJSON() =\n%#+v\nwant\n%#+v", w, tt.want)
			}
		})
	}

}
