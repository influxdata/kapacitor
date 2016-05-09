package stateful

import (
	"regexp"

	"github.com/influxdata/kapacitor/tick"
)

type operationKey struct {
	operator  tick.TokenType
	leftType  ValueType
	rightType ValueType
}

var boolTrueResultContainer = resultContainer{BoolValue: true, IsBoolValue: true}
var boolFalseResultContainer = resultContainer{BoolValue: false, IsBoolValue: true}
var emptyResultContainer = resultContainer{}

var evaluationFuncs = map[operationKey]evaluationFn{
	// -----------------------------------------
	//	Comparison evaluation funcs

	operationKey{operator: tick.TokenAnd, leftType: TBool, rightType: TBool}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left bool
		var right bool
		var err error

		if left, err = leftNode.EvalBool(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		// Short circuit evaluation
		if !left {
			return boolFalseResultContainer, nil
		}

		if right, err = rightNode.EvalBool(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left && right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenOr, leftType: TBool, rightType: TBool}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left bool
		var right bool
		var err error

		if left, err = leftNode.EvalBool(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		// Short circuit evaluation
		if left {
			return boolTrueResultContainer, nil
		}

		if right, err = rightNode.EvalBool(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left || right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenEqual, leftType: TBool, rightType: TBool}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left bool
		var right bool
		var err error

		if left, err = leftNode.EvalBool(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalBool(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left == right, IsBoolValue: true}, nil
	},

	operationKey{operator: tick.TokenNotEqual, leftType: TBool, rightType: TBool}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left bool
		var right bool
		var err error

		if left, err = leftNode.EvalBool(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalBool(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left != right, IsBoolValue: true}, nil
	},

	operationKey{operator: tick.TokenLess, leftType: TFloat64, rightType: TFloat64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left float64
		var right float64
		var err error

		if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left < right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenLessEqual, leftType: TInt64, rightType: TInt64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left int64
		var right int64
		var err error

		if left, err = leftNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left <= right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenNotEqual, leftType: TInt64, rightType: TFloat64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left int64
		var right float64
		var err error

		if left, err = leftNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: float64(left) != right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenGreaterEqual, leftType: TInt64, rightType: TInt64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left int64
		var right int64
		var err error

		if left, err = leftNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left >= right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenEqual, leftType: TFloat64, rightType: TFloat64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left float64
		var right float64
		var err error

		if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left == right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenNotEqual, leftType: TInt64, rightType: TInt64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left int64
		var right int64
		var err error

		if left, err = leftNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left != right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenNotEqual, leftType: TFloat64, rightType: TFloat64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left float64
		var right float64
		var err error

		if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left != right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenLessEqual, leftType: TFloat64, rightType: TFloat64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left float64
		var right float64
		var err error

		if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left <= right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenEqual, leftType: TInt64, rightType: TInt64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left int64
		var right int64
		var err error

		if left, err = leftNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left == right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenGreater, leftType: TInt64, rightType: TInt64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left int64
		var right int64
		var err error

		if left, err = leftNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left > right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenGreater, leftType: TFloat64, rightType: TInt64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left float64
		var right int64
		var err error

		if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left > float64(right), IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenGreaterEqual, leftType: TFloat64, rightType: TInt64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left float64
		var right int64
		var err error

		if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left >= float64(right), IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenEqual, leftType: TFloat64, rightType: TInt64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left float64
		var right int64
		var err error

		if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left == float64(right), IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenLessEqual, leftType: TInt64, rightType: TFloat64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left int64
		var right float64
		var err error

		if left, err = leftNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: float64(left) <= right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenEqual, leftType: TInt64, rightType: TFloat64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left int64
		var right float64
		var err error

		if left, err = leftNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: float64(left) == right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenNotEqual, leftType: TFloat64, rightType: TInt64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left float64
		var right int64
		var err error

		if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left != float64(right), IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenLess, leftType: TFloat64, rightType: TInt64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left float64
		var right int64
		var err error

		if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left < float64(right), IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenLess, leftType: TInt64, rightType: TInt64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left int64
		var right int64
		var err error

		if left, err = leftNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left < right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenGreaterEqual, leftType: TFloat64, rightType: TFloat64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left float64
		var right float64
		var err error

		if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left >= right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenGreater, leftType: TFloat64, rightType: TFloat64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left float64
		var right float64
		var err error

		if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left > right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenLessEqual, leftType: TFloat64, rightType: TInt64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left float64
		var right int64
		var err error

		if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left <= float64(right), IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenGreaterEqual, leftType: TInt64, rightType: TFloat64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left int64
		var right float64
		var err error

		if left, err = leftNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: float64(left) >= right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenGreater, leftType: TInt64, rightType: TFloat64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left int64
		var right float64
		var err error

		if left, err = leftNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: float64(left) > right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenLess, leftType: TInt64, rightType: TFloat64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left int64
		var right float64
		var err error

		if left, err = leftNode.EvalInt(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: float64(left) < right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenGreater, leftType: TString, rightType: TString}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left string
		var right string
		var err error

		if left, err = leftNode.EvalString(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalString(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left > right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenGreaterEqual, leftType: TString, rightType: TString}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left string
		var right string
		var err error

		if left, err = leftNode.EvalString(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalString(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left >= right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenLess, leftType: TString, rightType: TString}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left string
		var right string
		var err error

		if left, err = leftNode.EvalString(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalString(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left < right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenLessEqual, leftType: TString, rightType: TString}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left string
		var right string
		var err error

		if left, err = leftNode.EvalString(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalString(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left <= right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenEqual, leftType: TString, rightType: TString}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left string
		var right string
		var err error

		if left, err = leftNode.EvalString(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalString(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left == right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenNotEqual, leftType: TString, rightType: TString}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left string
		var right string
		var err error

		if left, err = leftNode.EvalString(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalString(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: left != right, IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenRegexNotEqual, leftType: TString, rightType: TRegex}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left string
		var right *regexp.Regexp
		var err error

		if left, err = leftNode.EvalString(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalRegex(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: !right.MatchString(left), IsBoolValue: true}, nil

	},

	operationKey{operator: tick.TokenRegexEqual, leftType: TString, rightType: TRegex}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left string
		var right *regexp.Regexp
		var err error

		if left, err = leftNode.EvalString(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalRegex(scope, executionState); err != nil {
			return boolFalseResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{BoolValue: right.MatchString(left), IsBoolValue: true}, nil

	},

	// -----------------------------------------
	//	Math evaluation funcs

	operationKey{operator: tick.TokenPlus, leftType: TFloat64, rightType: TFloat64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left float64
		var right float64
		var err error

		if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{Float64Value: left + right, IsFloat64Value: true}, nil
	},

	operationKey{operator: tick.TokenMinus, leftType: TFloat64, rightType: TFloat64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left float64
		var right float64
		var err error

		if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{Float64Value: left - right, IsFloat64Value: true}, nil
	},

	operationKey{operator: tick.TokenMult, leftType: TFloat64, rightType: TFloat64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left float64
		var right float64
		var err error

		if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{Float64Value: left * right, IsFloat64Value: true}, nil
	},

	operationKey{operator: tick.TokenDiv, leftType: TFloat64, rightType: TFloat64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left float64
		var right float64
		var err error

		if left, err = leftNode.EvalFloat(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalFloat(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{Float64Value: left / right, IsFloat64Value: true}, nil
	},

	operationKey{operator: tick.TokenPlus, leftType: TInt64, rightType: TInt64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left int64
		var right int64
		var err error

		if left, err = leftNode.EvalInt(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalInt(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{Int64Value: left + right, IsInt64Value: true}, nil
	},

	operationKey{operator: tick.TokenMinus, leftType: TInt64, rightType: TInt64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left int64
		var right int64
		var err error

		if left, err = leftNode.EvalInt(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalInt(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{Int64Value: left - right, IsInt64Value: true}, nil
	},

	operationKey{operator: tick.TokenMult, leftType: TInt64, rightType: TInt64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left int64
		var right int64
		var err error

		if left, err = leftNode.EvalInt(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalInt(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{Int64Value: left * right, IsInt64Value: true}, nil
	},

	operationKey{operator: tick.TokenDiv, leftType: TInt64, rightType: TInt64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left int64
		var right int64
		var err error

		if left, err = leftNode.EvalInt(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalInt(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{Int64Value: left / right, IsInt64Value: true}, nil
	},

	operationKey{operator: tick.TokenMod, leftType: TInt64, rightType: TInt64}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left int64
		var right int64
		var err error

		if left, err = leftNode.EvalInt(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalInt(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{Int64Value: left % right, IsInt64Value: true}, nil
	},

	// -----------------------------------------
	//	String concatenation func

	operationKey{operator: tick.TokenPlus, leftType: TString, rightType: TString}: func(scope *tick.Scope, executionState ExecutionState, leftNode, rightNode NodeEvaluator) (resultContainer, *ErrSide) {
		var left string
		var right string
		var err error

		if left, err = leftNode.EvalString(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsLeftSide: true}
		}

		if right, err = rightNode.EvalString(scope, executionState); err != nil {
			return emptyResultContainer, &ErrSide{error: err, IsRightSide: true}
		}

		return resultContainer{StringValue: left + right, IsStringValue: true}, nil
	},
}
