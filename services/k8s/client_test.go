package k8s_test

import (
	"testing"

	"github.com/influxdata/kapacitor/services/k8s"
)

func TestClient_Scales(t *testing.T) {
	cli, err := k8s.NewClient(k8s.Config{
		URLs: []string{"http://localhost:8001"},
	})
	if err != nil {
		t.Fatal(err)
	}

	scales := cli.Scales(k8s.NamespaceDefault)
	scale, err := scales.Get(k8s.DeploymentsKind, "hello-minikube")
	if err != nil {
		t.Fatal(err)
	}

	scale.Spec.Replicas = 6
	if err := scales.Update(k8s.DeploymentsKind, scale); err != nil {
		t.Fatal(err)
	}

	{
		scale, err := scales.Get(k8s.DeploymentsKind, "hello-minikube")
		if err != nil {
			t.Fatal(err)
		}
		if scale.Spec.Replicas != 6 {
			t.Errorf("unexpected spec replicas: got %d exp %d", scale.Spec.Replicas, 6)
		}
	}
}
