package client_test

import (
	"testing"

	"github.com/influxdata/kapacitor/services/k8s/client"
)

func TestClient_Scales(t *testing.T) {
	cli, err := client.New(client.Config{
		URLs: []string{"http://localhost:8001"},
	})
	if err != nil {
		t.Fatal(err)
	}

	scales := cli.Scales(client.NamespaceDefault)
	scale, err := scales.Get(client.DeploymentsKind, "hello-minikube")
	if err != nil {
		t.Fatal(err)
	}

	scale.Spec.Replicas = 6
	if err := scales.Update(client.DeploymentsKind, scale); err != nil {
		t.Fatal(err)
	}

	{
		scale, err := scales.Get(client.DeploymentsKind, "hello-minikube")
		if err != nil {
			t.Fatal(err)
		}
		if scale.Spec.Replicas != 6 {
			t.Errorf("unexpected spec replicas: got %d exp %d", scale.Spec.Replicas, 6)
		}
	}
}
