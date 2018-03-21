package kapacitor

import (
	"errors"
	"fmt"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

type EvalNode struct {
	node
	e           *pipeline.EvalNode
	expressions []stateful.Expression
	refVarList  [][]string
	scopePool   stateful.ScopePool
	tags        map[string]bool

	evalErrors *expvar.Int
}

// Create a new  EvalNode which applies a transformation func to each point in a stream and returns a single point.
func newEvalNode(et *ExecutingTask, n *pipeline.EvalNode, d NodeDiagnostic) (*EvalNode, error) {
	if len(n.AsList) != len(n.Lambdas) {
		return nil, errors.New("must provide one name per expression via the 'As' property")
	}
	en := &EvalNode{
		node: node{Node: n, et: et, diag: d},
		e:    n,
	}

	// Create stateful expressions
	en.expressions = make([]stateful.Expression, len(n.Lambdas))
	en.refVarList = make([][]string, len(n.Lambdas))
	expressions := make([]ast.Node, len(n.Lambdas))
	for i, lambda := range n.Lambdas {
		expressions[i] = lambda.Expression
		statefulExpr, err := stateful.NewExpression(lambda.Expression)
		if err != nil {
			return nil, fmt.Errorf("Failed to compile %v expression: %v", i, err)
		}
		en.expressions[i] = statefulExpr
		refVars := ast.FindReferenceVariables(lambda.Expression)
		en.refVarList[i] = refVars
	}
	// Create a single pool for the combination of all expressions
	en.scopePool = stateful.NewScopePool(ast.FindReferenceVariables(expressions...))

	// Create map of tags
	if l := len(n.TagsList); l > 0 {
		en.tags = make(map[string]bool, l)
		for _, tag := range n.TagsList {
			en.tags[tag] = true
		}
	}

	en.node.runF = en.runEval
	return en, nil
}

func (n *EvalNode) runEval(snapshot []byte) error {
	consumer := edge.NewGroupedConsumer(
		n.ins[0],
		n,
	)
	n.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())

	return consumer.Consume()

}

func (n *EvalNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	return edge.NewReceiverFromForwardReceiverWithStats(
		n.outs,
		edge.NewTimedForwardReceiver(n.timer, n.newGroup()),
	), nil
}

func (n *EvalNode) newGroup() *evalGroup {
	expressions := make([]stateful.Expression, len(n.expressions))
	for i, exp := range n.expressions {
		expressions[i] = exp.CopyReset()
	}
	return &evalGroup{
		n:           n,
		expressions: expressions,
	}
}

func (n *EvalNode) eval(expressions []stateful.Expression, p edge.FieldsTagsTimeSetter) error {

	vars := n.scopePool.Get()
	defer n.scopePool.Put(vars)

	for i, expr := range expressions {
		err := fillScope(vars, n.refVarList[i], p)
		if err != nil {
			return err
		}
		v, err := expr.Eval(vars)
		if err != nil {
			return err
		}
		name := n.e.AsList[i]
		vars.Set(name, v)
	}
	fields := p.Fields()
	tags := p.Tags()
	newTags := tags
	if len(n.tags) > 0 {
		newTags = newTags.Copy()
		for tag := range n.tags {
			v, err := vars.Get(tag)
			if err != nil {
				return err
			}
			if s, ok := v.(string); !ok {
				return fmt.Errorf("result of a tag expression must be of type string, got %T", v)
			} else {
				newTags[tag] = s
			}
		}
	}
	var newFields models.Fields
	if n.e.KeepFlag {
		if l := len(n.e.KeepList); l != 0 {
			newFields = make(models.Fields, l)
			for _, f := range n.e.KeepList {
				// Try the vars scope first
				if vars.Has(f) {
					v, err := vars.Get(f)
					if err != nil {
						return err
					}
					newFields[f] = v
				} else if v, ok := fields[f]; ok {
					// Try the raw fields next, since it may not have been a referenced var.
					newFields[f] = v
				} else {
					return fmt.Errorf("cannot keep field %q, field does not exist", f)
				}
			}
		} else {
			newFields = make(models.Fields, len(fields)+len(n.e.AsList))
			for f, v := range fields {
				newFields[f] = v
			}
			for _, f := range n.e.AsList {
				v, err := vars.Get(f)
				if err != nil {
					return err
				}
				newFields[f] = v
			}
		}
	} else {
		newFields = make(models.Fields, len(n.e.AsList)-len(n.tags))
		for _, f := range n.e.AsList {
			if n.tags[f] {
				continue
			}
			v, err := vars.Get(f)
			if err != nil {
				return err
			}
			newFields[f] = v
		}
	}
	p.SetFields(newFields)
	p.SetTags(newTags)
	return nil
}

type evalGroup struct {
	n           *EvalNode
	expressions []stateful.Expression
}

func (g *evalGroup) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	begin = begin.ShallowCopy()
	begin.SetSizeHint(0)
	return begin, nil
}

func (g *evalGroup) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	bp = bp.ShallowCopy()
	if g.doEval(bp) {
		return bp, nil
	}
	return nil, nil
}

func (g *evalGroup) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return end, nil
}

func (g *evalGroup) Point(p edge.PointMessage) (edge.Message, error) {
	p = p.ShallowCopy()
	if g.doEval(p) {
		return p, nil
	}
	return nil, nil
}

func (g *evalGroup) doEval(p edge.FieldsTagsTimeSetter) bool {
	err := g.n.eval(g.expressions, p)
	if err != nil {
		if !g.n.e.QuietFlag {
			g.n.diag.Error("error evaluating expression", err)
		}
		// Skip bad point
		return false
	}
	return true
}

func (g *evalGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (g *evalGroup) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (g *evalGroup) Done() {}
