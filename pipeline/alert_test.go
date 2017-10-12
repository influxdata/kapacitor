package pipeline

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestAlertNode_MarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		node    AlertNode
		want    string
		wantErr bool
	}{
		{
			name: "marshal post",
			node: AlertNode{
				AlertNodeData: &AlertNodeData{
					HTTPPostHandlers: []*AlertHTTPPostHandler{
						{
							URL:      "http://howdy.local",
							Endpoint: "/endpoint",
						},
					},
				},
			},
			want: `{
    "typeOf": "alert",
    "id": "0",
    "topic": "",
    "alertId": "",
    "message": "",
    "details": "",
    "info": null,
    "warn": null,
    "crit": null,
    "infoReset": null,
    "warnReset": null,
    "critReset": null,
    "useFlapping": false,
    "flapLow": 0,
    "flapHigh": 0,
    "history": 0,
    "levelTag": "",
    "levelField": "",
    "messageField": "",
    "durationField": "",
    "idTag": "",
    "idField": "",
    "all": false,
    "noRecoveries": false,
    "stateChangesOnly": false,
    "stateChangesOnlyDuration": 0,
    "post": [
        {
            "url": "http://howdy.local",
            "endpoint": "/endpoint",
            "headers": null
        }
    ],
    "tcp": null,
    "email": null,
    "exec": null,
    "log": null,
    "victorOps": null,
    "pagerDuty": null,
    "pushover": null,
    "sensu": null,
    "slack": null,
    "telegram": null,
    "hipChat": null,
    "alerta": null,
    "opsGenie": null,
    "talk": null,
    "mqtt": null,
    "snmpTrap": null
}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.MarshalIndent(&tt.node, "", "    ")
			if (err != nil) != tt.wantErr {
				t.Errorf("AlertNode.MarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if string(got) != tt.want {
				fmt.Println(string(got))
				t.Errorf("AlertNode.MarshalJSON() = %s, want %s", string(got), tt.want)
			}
		})
	}
}

/*
func TestAlertHTTPPostHandler_MarshalJSON(t *testing.T) {
	type fields struct {
		AlertNode *AlertNode
		URL       string
		Endpoint  string
		Headers   map[string]string
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &AlertHTTPPostHandler{
				AlertNode: tt.fields.AlertNode,
				URL:       tt.fields.URL,
				Endpoint:  tt.fields.Endpoint,
				Headers:   tt.fields.Headers,
			}
			got, err := a.MarshalJSON()
			if (err != nil) != tt.wantErr {
				t.Errorf("AlertHTTPPostHandler.MarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AlertHTTPPostHandler.MarshalJSON() = %v, want %v", got, tt.want)
			}
		})
	}
}
*/
