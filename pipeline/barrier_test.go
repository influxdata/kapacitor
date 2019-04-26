package pipeline

import (
	"testing"
	"time"
)

func TestBarrierNode_MarshalJSON(t *testing.T) {
	type fields struct {
		Period time.Duration
		Idle   time.Duration
		Delete bool
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
				Period: time.Hour,
				Idle:   time.Minute,
				Delete: true,
			},
			want: `{"typeOf":"barrier","id":"0","delete":true,"period":"1h","idle":"1m"}`,
		},
		{
			name: "only period ",
			fields: fields{
				Period: time.Hour,
			},
			want: `{"typeOf":"barrier","id":"0","period":"1h","idle":"0s"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBarrierNode(StreamEdge)
			b.Period = tt.fields.Period
			b.Idle = tt.fields.Idle
			b.Delete = tt.fields.Delete
			MarshalTestHelper(t, b, tt.wantErr, tt.want)
		})
	}
}
