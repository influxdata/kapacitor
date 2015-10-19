package expr

// Expression functions are stateful. Their state is updated with
// each call to the function. A StatefulExpr is an expression tree
// and its associated function state.
type StatefulExpr struct {
	Tree  *Tree
	Funcs Funcs
}

// Reset the state
func (s *StatefulExpr) Reset() {
	for _, f := range s.Funcs {
		f.Reset()
	}
}

func (s *StatefulExpr) EvalBool(v Vars) (bool, error) {
	return s.Tree.EvalBool(v, s.Funcs)
}

func (s *StatefulExpr) EvalNum(v Vars) (float64, error) {
	return s.Tree.EvalNum(v, s.Funcs)
}
