package stateful

import (
	"regexp"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

type operationKey struct {
	operator  ast.TokenType
	leftType  ast.ValueType
	rightType ast.ValueType
}

var boolTrueResultContainer = resultContainer{BoolValue: true, IsBoolValue: true}
var boolFalseResultContainer = resultContainer{BoolValue: false, IsBoolValue: true}
var emptyResultContainer = resultContainer{}

type evaluationFnInfo struct {
	f          evaluationFn
	returnType ast.ValueType
}

// Constant return types of all binary operations
var binaryConstantTypes map[operationKey]ast.ValueType

func init() {
	// Populate binaryConstantTypes from the evaluationFuncs map
	binaryConstantTypes = make(map[operationKey]ast.ValueType, len(evaluationFuncs))
	for opKey, info := range evaluationFuncs {
		binaryConstantTypes[opKey] = info.returnType
	}
}

var evaluationFuncs = map[operationKey]*evaluationFnInfo{
	// -----------------------------------------
	//	Comparison evaluation funcs

	operationKey{operator: ast.TokenAnd, leftType: ast.TBool, rightType: ast.TBool}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left bool
			var right bool
			var err error

			if left, err = leftNode.EvalBool(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			// Short circuit evaluation
			if !left {
				return boolFalseResultContainer, nil
			}

			if right, err = rightNode.EvalBool(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left && right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenOr, leftType: ast.TBool, rightType: ast.TBool}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left bool
			var right bool
			var err error

			if left, err = leftNode.EvalBool(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			// Short circuit evaluation
			if left {
				return boolTrueResultContainer, nil
			}

			if right, err = rightNode.EvalBool(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left || right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenEqual, leftType: ast.TBool, rightType: ast.TBool}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left bool
			var right bool
			var err error

			if left, err = leftNode.EvalBool(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalBool(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left == right, IsBoolValue: true}, nil
		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenNotEqual, leftType: ast.TBool, rightType: ast.TBool}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left bool
			var right bool
			var err error

			if left, err = leftNode.EvalBool(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalBool(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left != right, IsBoolValue: true}, nil
		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenLess, leftType: ast.TFloat, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left float64
			var right float64
			var err error

			if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left < right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenLessEqual, leftType: ast.TInt, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right int64
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left <= right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenNotEqual, leftType: ast.TInt, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right float64
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: float64(left) != right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenGreaterEqual, leftType: ast.TInt, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right int64
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left >= right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenEqual, leftType: ast.TFloat, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left float64
			var right float64
			var err error

			if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left == right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenNotEqual, leftType: ast.TInt, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right int64
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left != right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenNotEqual, leftType: ast.TFloat, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left float64
			var right float64
			var err error

			if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left != right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenLessEqual, leftType: ast.TFloat, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left float64
			var right float64
			var err error

			if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left <= right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenEqual, leftType: ast.TInt, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right int64
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left == right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenGreater, leftType: ast.TInt, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right int64
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left > right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenGreater, leftType: ast.TFloat, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left float64
			var right int64
			var err error

			if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left > float64(right), IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenGreaterEqual, leftType: ast.TFloat, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left float64
			var right int64
			var err error

			if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left >= float64(right), IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenEqual, leftType: ast.TFloat, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left float64
			var right int64
			var err error

			if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left == float64(right), IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenLessEqual, leftType: ast.TInt, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right float64
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: float64(left) <= right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenEqual, leftType: ast.TInt, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right float64
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: float64(left) == right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenNotEqual, leftType: ast.TFloat, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left float64
			var right int64
			var err error

			if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left != float64(right), IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenLess, leftType: ast.TFloat, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left float64
			var right int64
			var err error

			if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left < float64(right), IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenLess, leftType: ast.TInt, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right int64
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left < right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenGreaterEqual, leftType: ast.TFloat, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left float64
			var right float64
			var err error

			if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left >= right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenGreater, leftType: ast.TFloat, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left float64
			var right float64
			var err error

			if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left > right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenLessEqual, leftType: ast.TFloat, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left float64
			var right int64
			var err error

			if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left <= float64(right), IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenGreaterEqual, leftType: ast.TInt, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right float64
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: float64(left) >= right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenGreater, leftType: ast.TInt, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right float64
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: float64(left) > right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenLess, leftType: ast.TInt, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right float64
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: float64(left) < right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenGreater, leftType: ast.TString, rightType: ast.TString}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left string
			var right string
			var err error

			if left, err = leftNode.EvalString(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalString(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left > right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenGreaterEqual, leftType: ast.TString, rightType: ast.TString}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left string
			var right string
			var err error

			if left, err = leftNode.EvalString(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalString(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left >= right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenLess, leftType: ast.TString, rightType: ast.TString}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left string
			var right string
			var err error

			if left, err = leftNode.EvalString(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalString(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left < right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenLessEqual, leftType: ast.TString, rightType: ast.TString}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left string
			var right string
			var err error

			if left, err = leftNode.EvalString(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalString(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left <= right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenEqual, leftType: ast.TString, rightType: ast.TString}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left string
			var right string
			var err error

			if left, err = leftNode.EvalString(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalString(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left == right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenNotEqual, leftType: ast.TString, rightType: ast.TString}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left string
			var right string
			var err error

			if left, err = leftNode.EvalString(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalString(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left != right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenRegexNotEqual, leftType: ast.TString, rightType: ast.TRegex}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left string
			var right *regexp.Regexp
			var err error

			if left, err = leftNode.EvalString(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalRegex(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: !right.MatchString(left), IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenRegexEqual, leftType: ast.TString, rightType: ast.TRegex}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left string
			var right *regexp.Regexp
			var err error

			if left, err = leftNode.EvalString(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalRegex(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: right.MatchString(left), IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenEqual, leftType: ast.TDuration, rightType: ast.TDuration}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left time.Duration
			var right time.Duration
			var err error

			if left, err = leftNode.EvalDuration(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalDuration(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left == right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenNotEqual, leftType: ast.TDuration, rightType: ast.TDuration}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left time.Duration
			var right time.Duration
			var err error

			if left, err = leftNode.EvalDuration(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalDuration(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left != right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},
	operationKey{operator: ast.TokenGreater, leftType: ast.TDuration, rightType: ast.TDuration}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left time.Duration
			var right time.Duration
			var err error

			if left, err = leftNode.EvalDuration(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalDuration(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left > right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenGreaterEqual, leftType: ast.TDuration, rightType: ast.TDuration}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left time.Duration
			var right time.Duration
			var err error

			if left, err = leftNode.EvalDuration(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalDuration(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left >= right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenLess, leftType: ast.TDuration, rightType: ast.TDuration}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left time.Duration
			var right time.Duration
			var err error

			if left, err = leftNode.EvalDuration(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalDuration(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left < right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	operationKey{operator: ast.TokenLessEqual, leftType: ast.TDuration, rightType: ast.TDuration}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left time.Duration
			var right time.Duration
			var err error

			if left, err = leftNode.EvalDuration(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalDuration(scope, executionState); err != nil {
				return boolFalseResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{BoolValue: left <= right, IsBoolValue: true}, nil

		},
		returnType: ast.TBool,
	},

	// -----------------------------------------
	//	Math evaluation funcs

	operationKey{operator: ast.TokenPlus, leftType: ast.TFloat, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left float64
			var right float64
			var err error

			if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{Float64Value: left + right, IsFloat64Value: true}, nil
		},
		returnType: ast.TFloat,
	},

	operationKey{operator: ast.TokenMinus, leftType: ast.TFloat, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left float64
			var right float64
			var err error

			if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{Float64Value: left - right, IsFloat64Value: true}, nil
		},
		returnType: ast.TFloat,
	},

	operationKey{operator: ast.TokenMult, leftType: ast.TFloat, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left float64
			var right float64
			var err error

			if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{Float64Value: left * right, IsFloat64Value: true}, nil
		},
		returnType: ast.TFloat,
	},

	operationKey{operator: ast.TokenDiv, leftType: ast.TFloat, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left float64
			var right float64
			var err error

			if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{Float64Value: left / right, IsFloat64Value: true}, nil
		},
		returnType: ast.TFloat,
	},

	operationKey{operator: ast.TokenPlus, leftType: ast.TInt, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right int64
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{Int64Value: left + right, IsInt64Value: true}, nil
		},
		returnType: ast.TInt,
	},

	operationKey{operator: ast.TokenMinus, leftType: ast.TInt, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right int64
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{Int64Value: left - right, IsInt64Value: true}, nil
		},
		returnType: ast.TInt,
	},

	operationKey{operator: ast.TokenMult, leftType: ast.TInt, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right int64
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{Int64Value: left * right, IsInt64Value: true}, nil
		},
		returnType: ast.TInt,
	},

	operationKey{operator: ast.TokenDiv, leftType: ast.TInt, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right int64
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{Int64Value: left / right, IsInt64Value: true}, nil
		},
		returnType: ast.TInt,
	},

	operationKey{operator: ast.TokenMod, leftType: ast.TInt, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right int64
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{Int64Value: left % right, IsInt64Value: true}, nil
		},
		returnType: ast.TInt,
	},

	operationKey{operator: ast.TokenPlus, leftType: ast.TDuration, rightType: ast.TDuration}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left time.Duration
			var right time.Duration
			var err error

			if left, err = leftNode.EvalDuration(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalDuration(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{DurationValue: left + right, IsDurationValue: true}, nil
		},
		returnType: ast.TDuration,
	},

	operationKey{operator: ast.TokenMinus, leftType: ast.TDuration, rightType: ast.TDuration}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left time.Duration
			var right time.Duration
			var err error

			if left, err = leftNode.EvalDuration(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalDuration(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{DurationValue: left - right, IsDurationValue: true}, nil
		},
		returnType: ast.TDuration,
	},

	operationKey{operator: ast.TokenMult, leftType: ast.TDuration, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left time.Duration
			var right int64
			var err error

			if left, err = leftNode.EvalDuration(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{DurationValue: left * time.Duration(right), IsDurationValue: true}, nil
		},
		returnType: ast.TDuration,
	},

	operationKey{operator: ast.TokenMult, leftType: ast.TInt, rightType: ast.TDuration}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left int64
			var right time.Duration
			var err error

			if left, err = leftNode.EvalInt(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalDuration(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{DurationValue: time.Duration(left) * right, IsDurationValue: true}, nil
		},
		returnType: ast.TDuration,
	},

	operationKey{operator: ast.TokenMult, leftType: ast.TDuration, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left time.Duration
			var right float64
			var err error

			if left, err = leftNode.EvalDuration(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{DurationValue: time.Duration(float64(left) * right), IsDurationValue: true}, nil
		},
		returnType: ast.TDuration,
	},

	operationKey{operator: ast.TokenMult, leftType: ast.TFloat, rightType: ast.TDuration}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left float64
			var right time.Duration
			var err error

			if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalDuration(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{DurationValue: time.Duration(left * float64(right)), IsDurationValue: true}, nil
		},
		returnType: ast.TDuration,
	},

	operationKey{operator: ast.TokenDiv, leftType: ast.TDuration, rightType: ast.TInt}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left time.Duration
			var right int64
			var err error

			if left, err = leftNode.EvalDuration(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalInt(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{DurationValue: left / time.Duration(right), IsDurationValue: true}, nil
		},
		returnType: ast.TDuration,
	},
	operationKey{operator: ast.TokenDiv, leftType: ast.TDuration, rightType: ast.TFloat}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left time.Duration
			var right float64
			var err error

			if left, err = leftNode.EvalDuration(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{DurationValue: time.Duration(float64(left) / right), IsDurationValue: true}, nil
		},
		returnType: ast.TDuration,
	},
	operationKey{operator: ast.TokenDiv, leftType: ast.TDuration, rightType: ast.TDuration}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left time.Duration
			var right time.Duration
			var err error

			if left, err = leftNode.EvalDuration(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalDuration(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{Int64Value: int64(left / right), IsInt64Value: true}, nil
		},
		returnType: ast.TInt,
	},

	// -----------------------------------------
	//	String concatenation func

	operationKey{operator: ast.TokenPlus, leftType: ast.TString, rightType: ast.TString}: {
		f: func(scope *Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
			var left string
			var right string
			var err error

			if left, err = leftNode.EvalString(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
			}

			if right, err = rightNode.EvalString(scope, executionState); err != nil {
				return emptyResultContainer, &ErrSide{error: err, IsRight: true}
			}

			return resultContainer{StringValue: left + right, IsStringValue: true}, nil
		},
		returnType: ast.TString,
	},
}
