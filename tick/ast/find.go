package ast

// FindReferenceVariables walks all nodes and returns a list of name from reference variables.
func FindReferenceVariables(nodes ...Node) []string {
	variablesSet := make(map[string]bool)

	for _, node := range nodes {
		Walk(node, func(n Node) (Node, error) {
			if ref, ok := n.(*ReferenceNode); ok {
				variablesSet[ref.Reference] = true
			}
			return n, nil
		})
	}

	variables := make([]string, 0, len(variablesSet))

	for variable := range variablesSet {
		variables = append(variables, variable)
	}

	return variables
}

// FindFunctionCalls walks all nodes and returns a list of name of function calls.
func FindFunctionCalls(nodes ...Node) []string {
	funcCallsSet := make(map[string]bool)

	for _, node := range nodes {
		Walk(node, func(n Node) (Node, error) {
			if fnc, ok := n.(*FunctionNode); ok {
				funcCallsSet[fnc.Func] = true
			}
			return n, nil
		})
	}

	funcCalls := make([]string, 0, len(funcCallsSet))

	for variable := range funcCallsSet {
		funcCalls = append(funcCalls, variable)
	}

	return funcCalls
}
