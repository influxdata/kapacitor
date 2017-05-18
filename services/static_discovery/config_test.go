package static_discovery

import (
	"reflect"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
)

func TestConfig_PromConfig(t *testing.T) {
	type fields struct {
		Enabled bool
		ID      string
		Targets []string
		Labels  map[string]string
	}
	tests := []struct {
		name   string
		fields fields
		want   []*config.TargetGroup
	}{
		{
			name: "Test Address Label",
			fields: fields{
				ID: "mylocalhost",
				Targets: []string{
					"localhost:9100",
					"localhost:9200",
				},
				Labels: map[string]string{
					"my":      "neat",
					"metrics": "host",
				},
			},
			want: []*config.TargetGroup{
				&config.TargetGroup{
					Source: "mylocalhost",
					Targets: []model.LabelSet{
						{model.AddressLabel: "localhost:9100"},
						{model.AddressLabel: "localhost:9200"},
					},
					Labels: model.LabelSet{
						"my":      "neat",
						"metrics": "host",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := Config{
				Enabled: tt.fields.Enabled,
				ID:      tt.fields.ID,
				Targets: tt.fields.Targets,
				Labels:  tt.fields.Labels,
			}
			if got := s.PromConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Config.PromConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}
