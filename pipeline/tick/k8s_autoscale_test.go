package tick_test

import (
	"testing"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

func TestK8sAutoscaleResourceName(t *testing.T) {
	pipe, _, from := StreamFrom()
	n := from.K8sAutoscale()

	n.Cluster = "marina"
	n.Namespace = "winona_marina"
	n.Kind = "deployments"
	n.ResourceName = "east_docks"
	n.CurrentField = "replicas"
	n.Max = 10
	n.Min = 5
	n.IncreaseCooldown = time.Hour
	n.DecreaseCooldown = time.Minute
	n.NamespaceTag = "lucky_find"
	n.KindTag = "deployment"
	n.ResourceTag = "dock"
	n.Replicas = &ast.LambdaNode{
		Expression: &ast.FunctionNode{
			Type: ast.GlobalFunc,
			Func: "if",
			Args: []ast.Node{
				&ast.BinaryNode{
					Operator: ast.TokenGreater,
					Left: &ast.ReferenceNode{
						Reference: "greater spire",
					},
					Right: &ast.NumberNode{
						IsInt: true,
						Int64: 1,
						Base:  10,
					},
				},
				&ast.BinaryNode{
					Operator: ast.TokenPlus,
					Left: &ast.ReferenceNode{
						Reference: "replicas",
					},
					Right: &ast.NumberNode{
						IsInt: true,
						Int64: 1,
						Base:  10,
					},
				},
				&ast.ReferenceNode{
					Reference: "replicas",
				},
			},
		},
	}

	want := `stream
    |from()
    |k8sAutoscale()
        .cluster('marina')
        .namespace('winona_marina')
        .kind('deployments')
        .resourceName('east_docks')
        .currentField('replicas')
        .max(10)
        .min(5)
        .replicas(lambda: if("greater spire" > 1, "replicas" + 1, "replicas"))
        .increaseCooldown(1h)
        .decreaseCooldown(1m)
        .namespaceTag('lucky_find')
        .kindTag('deployment')
        .resourceTag('dock')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestK8sAutoscaleResourceNameTag(t *testing.T) {
	pipe, _, from := StreamFrom()
	n := from.K8sAutoscale()

	n.Cluster = "marina"
	n.Namespace = "winona_marina"
	n.Kind = "deployments"
	n.ResourceNameTag = "docks"
	n.CurrentField = "replicas"
	n.Max = 10
	n.Min = 5
	n.IncreaseCooldown = time.Hour
	n.DecreaseCooldown = time.Minute
	n.NamespaceTag = "lucky_find"
	n.KindTag = "deployment"
	n.ResourceTag = "dock"
	n.Replicas = &ast.LambdaNode{
		Expression: &ast.FunctionNode{
			Type: ast.GlobalFunc,
			Func: "if",
			Args: []ast.Node{
				&ast.BinaryNode{
					Operator: ast.TokenGreater,
					Left: &ast.ReferenceNode{
						Reference: "greater spire",
					},
					Right: &ast.NumberNode{
						IsInt: true,
						Int64: 1,
						Base:  10,
					},
				},
				&ast.BinaryNode{
					Operator: ast.TokenPlus,
					Left: &ast.ReferenceNode{
						Reference: "replicas",
					},
					Right: &ast.NumberNode{
						IsInt: true,
						Int64: 1,
						Base:  10,
					},
				},
				&ast.ReferenceNode{
					Reference: "replicas",
				},
			},
		},
	}

	want := `stream
    |from()
    |k8sAutoscale()
        .cluster('marina')
        .namespace('winona_marina')
        .kind('deployments')
        .resourceNameTag('docks')
        .currentField('replicas')
        .max(10)
        .min(5)
        .replicas(lambda: if("greater spire" > 1, "replicas" + 1, "replicas"))
        .increaseCooldown(1h)
        .decreaseCooldown(1m)
        .namespaceTag('lucky_find')
        .kindTag('deployment')
        .resourceTag('dock')
`
	PipelineTickTestHelper(t, pipe, want)
}
