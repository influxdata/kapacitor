package pipeline

import (
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
    "category": "",
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
    "inhibitors": null,
    "post": [
        {
            "url": "http://howdy.local",
            "endpoint": "/endpoint",
            "headers": null,
            "captureResponse": false,
            "timeout": 0,
            "skipSSLVerification": false
        }
    ],
    "tcp": null,
    "email": null,
    "exec": null,
    "log": null,
    "victorOps": null,
    "pagerDuty": null,
    "pagerDuty2": null,
    "pushover": null,
    "sensu": null,
    "slack": null,
    "discord": null,
    "bigPanda": null,
    "telegram": null,
    "hipChat": null,
    "alerta": null,
    "opsGenie": null,
    "opsGenie2": null,
    "talk": null,
    "mqtt": null,
    "snmpTrap": null,
    "kafka": null,
    "teams": null,
    "serviceNow": null,
    "zenoss": null
}`,
		},
		{
			name: "marshal kafka with partition-by-id enabled",
			node: AlertNode{
				AlertNodeData: &AlertNodeData{
					KafkaHandlers: []*KafkaHandler{
						{
							Cluster:                "my-cluster",
							KafkaTopic:             "my-topic",
							IsDisablePartitionById: false,
							PartitionHashAlgorithm: "murmur2",
						},
					},
				},
			},
			want: `{
    "typeOf": "alert",
    "id": "0",
    "category": "",
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
    "inhibitors": null,
    "post": null,
    "tcp": null,
    "email": null,
    "exec": null,
    "log": null,
    "victorOps": null,
    "pagerDuty": null,
    "pagerDuty2": null,
    "pushover": null,
    "sensu": null,
    "slack": null,
    "discord": null,
    "bigPanda": null,
    "telegram": null,
    "hipChat": null,
    "alerta": null,
    "opsGenie": null,
    "opsGenie2": null,
    "talk": null,
    "mqtt": null,
    "snmpTrap": null,
    "kafka": [
        {
            "cluster": "my-cluster",
            "kafka-topic": "my-topic",
            "disable-partition-by-id": false,
            "partition-hash-algorithm": "murmur2"
        }
    ],
    "teams": null,
    "serviceNow": null,
    "zenoss": null
}`,
		},
		{
			name: "marshal kafka with partition-by-id disabled",
			node: AlertNode{
				AlertNodeData: &AlertNodeData{
					KafkaHandlers: []*KafkaHandler{
						{
							Cluster:                "my-cluster",
							KafkaTopic:             "my-topic",
							IsDisablePartitionById: true,
						},
					},
				},
			},
			want: `{
    "typeOf": "alert",
    "id": "0",
    "category": "",
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
    "inhibitors": null,
    "post": null,
    "tcp": null,
    "email": null,
    "exec": null,
    "log": null,
    "victorOps": null,
    "pagerDuty": null,
    "pagerDuty2": null,
    "pushover": null,
    "sensu": null,
    "slack": null,
    "discord": null,
    "bigPanda": null,
    "telegram": null,
    "hipChat": null,
    "alerta": null,
    "opsGenie": null,
    "opsGenie2": null,
    "talk": null,
    "mqtt": null,
    "snmpTrap": null,
    "kafka": [
        {
            "cluster": "my-cluster",
            "kafka-topic": "my-topic",
            "disable-partition-by-id": true
        }
    ],
    "teams": null,
    "serviceNow": null,
    "zenoss": null
}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			MarshalIndentTestHelper(t, &tt.node, tt.wantErr, tt.want)
		})
	}
}
