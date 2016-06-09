// Generated by tmpl
// https://github.com/benbjohnson/tmpl
//
// DO NOT EDIT!
// Source: functions.gen.go.tmpl

package influxql

import "sort"

// FloatPointAggregator aggregates points to produce a single point.
type FloatPointAggregator interface {
	AggregateFloat(p *FloatPoint)
}

// FloatBulkPointAggregator aggregates multiple points at a time.
type FloatBulkPointAggregator interface {
	AggregateFloatBulk(points []FloatPoint)
}

// AggregateFloatPoints feeds a slice of FloatPoint into an
// aggregator. If the aggregator is a FloatBulkPointAggregator, it will
// use the AggregateBulk method.
func AggregateFloatPoints(a FloatPointAggregator, points []FloatPoint) {
	switch a := a.(type) {
	case FloatBulkPointAggregator:
		a.AggregateFloatBulk(points)
	default:
		for _, p := range points {
			a.AggregateFloat(&p)
		}
	}
}

// FloatPointEmitter produces a single point from an aggregate.
type FloatPointEmitter interface {
	Emit() []FloatPoint
}

// FloatReduceFunc is the function called by a FloatPoint reducer.
type FloatReduceFunc func(prev *FloatPoint, curr *FloatPoint) (t int64, v float64, aux []interface{})

type FloatFuncReducer struct {
	prev *FloatPoint
	fn   FloatReduceFunc
}

func NewFloatFuncReducer(fn FloatReduceFunc, prev *FloatPoint) *FloatFuncReducer {
	return &FloatFuncReducer{fn: fn, prev: prev}
}

func (r *FloatFuncReducer) AggregateFloat(p *FloatPoint) {
	t, v, aux := r.fn(r.prev, p)
	if r.prev == nil {
		r.prev = &FloatPoint{}
	}
	r.prev.Time = t
	r.prev.Value = v
	r.prev.Aux = aux
	if p.Aggregated > 1 {
		r.prev.Aggregated += p.Aggregated
	} else {
		r.prev.Aggregated++
	}
}

func (r *FloatFuncReducer) Emit() []FloatPoint {
	return []FloatPoint{*r.prev}
}

// FloatReduceSliceFunc is the function called by a FloatPoint reducer.
type FloatReduceSliceFunc func(a []FloatPoint) []FloatPoint

type FloatSliceFuncReducer struct {
	points []FloatPoint
	fn     FloatReduceSliceFunc
}

func NewFloatSliceFuncReducer(fn FloatReduceSliceFunc) *FloatSliceFuncReducer {
	return &FloatSliceFuncReducer{fn: fn}
}

func (r *FloatSliceFuncReducer) AggregateFloat(p *FloatPoint) {
	r.points = append(r.points, *p)
}

func (r *FloatSliceFuncReducer) AggregateFloatBulk(points []FloatPoint) {
	r.points = append(r.points, points...)
}

func (r *FloatSliceFuncReducer) Emit() []FloatPoint {
	return r.fn(r.points)
}

// FloatReduceIntegerFunc is the function called by a FloatPoint reducer.
type FloatReduceIntegerFunc func(prev *IntegerPoint, curr *FloatPoint) (t int64, v int64, aux []interface{})

type FloatFuncIntegerReducer struct {
	prev *IntegerPoint
	fn   FloatReduceIntegerFunc
}

func NewFloatFuncIntegerReducer(fn FloatReduceIntegerFunc, prev *IntegerPoint) *FloatFuncIntegerReducer {
	return &FloatFuncIntegerReducer{fn: fn, prev: prev}
}

func (r *FloatFuncIntegerReducer) AggregateFloat(p *FloatPoint) {
	t, v, aux := r.fn(r.prev, p)
	if r.prev == nil {
		r.prev = &IntegerPoint{}
	}
	r.prev.Time = t
	r.prev.Value = v
	r.prev.Aux = aux
	if p.Aggregated > 1 {
		r.prev.Aggregated += p.Aggregated
	} else {
		r.prev.Aggregated++
	}
}

func (r *FloatFuncIntegerReducer) Emit() []IntegerPoint {
	return []IntegerPoint{*r.prev}
}

// FloatReduceIntegerSliceFunc is the function called by a FloatPoint reducer.
type FloatReduceIntegerSliceFunc func(a []FloatPoint) []IntegerPoint

type FloatSliceFuncIntegerReducer struct {
	points []FloatPoint
	fn     FloatReduceIntegerSliceFunc
}

func NewFloatSliceFuncIntegerReducer(fn FloatReduceIntegerSliceFunc) *FloatSliceFuncIntegerReducer {
	return &FloatSliceFuncIntegerReducer{fn: fn}
}

func (r *FloatSliceFuncIntegerReducer) AggregateFloat(p *FloatPoint) {
	r.points = append(r.points, *p)
}

func (r *FloatSliceFuncIntegerReducer) AggregateFloatBulk(points []FloatPoint) {
	r.points = append(r.points, points...)
}

func (r *FloatSliceFuncIntegerReducer) Emit() []IntegerPoint {
	return r.fn(r.points)
}

// FloatReduceStringFunc is the function called by a FloatPoint reducer.
type FloatReduceStringFunc func(prev *StringPoint, curr *FloatPoint) (t int64, v string, aux []interface{})

type FloatFuncStringReducer struct {
	prev *StringPoint
	fn   FloatReduceStringFunc
}

func NewFloatFuncStringReducer(fn FloatReduceStringFunc, prev *StringPoint) *FloatFuncStringReducer {
	return &FloatFuncStringReducer{fn: fn, prev: prev}
}

func (r *FloatFuncStringReducer) AggregateFloat(p *FloatPoint) {
	t, v, aux := r.fn(r.prev, p)
	if r.prev == nil {
		r.prev = &StringPoint{}
	}
	r.prev.Time = t
	r.prev.Value = v
	r.prev.Aux = aux
	if p.Aggregated > 1 {
		r.prev.Aggregated += p.Aggregated
	} else {
		r.prev.Aggregated++
	}
}

func (r *FloatFuncStringReducer) Emit() []StringPoint {
	return []StringPoint{*r.prev}
}

// FloatReduceStringSliceFunc is the function called by a FloatPoint reducer.
type FloatReduceStringSliceFunc func(a []FloatPoint) []StringPoint

type FloatSliceFuncStringReducer struct {
	points []FloatPoint
	fn     FloatReduceStringSliceFunc
}

func NewFloatSliceFuncStringReducer(fn FloatReduceStringSliceFunc) *FloatSliceFuncStringReducer {
	return &FloatSliceFuncStringReducer{fn: fn}
}

func (r *FloatSliceFuncStringReducer) AggregateFloat(p *FloatPoint) {
	r.points = append(r.points, *p)
}

func (r *FloatSliceFuncStringReducer) AggregateFloatBulk(points []FloatPoint) {
	r.points = append(r.points, points...)
}

func (r *FloatSliceFuncStringReducer) Emit() []StringPoint {
	return r.fn(r.points)
}

// FloatReduceBooleanFunc is the function called by a FloatPoint reducer.
type FloatReduceBooleanFunc func(prev *BooleanPoint, curr *FloatPoint) (t int64, v bool, aux []interface{})

type FloatFuncBooleanReducer struct {
	prev *BooleanPoint
	fn   FloatReduceBooleanFunc
}

func NewFloatFuncBooleanReducer(fn FloatReduceBooleanFunc, prev *BooleanPoint) *FloatFuncBooleanReducer {
	return &FloatFuncBooleanReducer{fn: fn, prev: prev}
}

func (r *FloatFuncBooleanReducer) AggregateFloat(p *FloatPoint) {
	t, v, aux := r.fn(r.prev, p)
	if r.prev == nil {
		r.prev = &BooleanPoint{}
	}
	r.prev.Time = t
	r.prev.Value = v
	r.prev.Aux = aux
	if p.Aggregated > 1 {
		r.prev.Aggregated += p.Aggregated
	} else {
		r.prev.Aggregated++
	}
}

func (r *FloatFuncBooleanReducer) Emit() []BooleanPoint {
	return []BooleanPoint{*r.prev}
}

// FloatReduceBooleanSliceFunc is the function called by a FloatPoint reducer.
type FloatReduceBooleanSliceFunc func(a []FloatPoint) []BooleanPoint

type FloatSliceFuncBooleanReducer struct {
	points []FloatPoint
	fn     FloatReduceBooleanSliceFunc
}

func NewFloatSliceFuncBooleanReducer(fn FloatReduceBooleanSliceFunc) *FloatSliceFuncBooleanReducer {
	return &FloatSliceFuncBooleanReducer{fn: fn}
}

func (r *FloatSliceFuncBooleanReducer) AggregateFloat(p *FloatPoint) {
	r.points = append(r.points, *p)
}

func (r *FloatSliceFuncBooleanReducer) AggregateFloatBulk(points []FloatPoint) {
	r.points = append(r.points, points...)
}

func (r *FloatSliceFuncBooleanReducer) Emit() []BooleanPoint {
	return r.fn(r.points)
}

// FloatDistinctReducer returns the distinct points in a series.
type FloatDistinctReducer struct {
	m map[float64]FloatPoint
}

// NewFloatDistinctReducer creates a new FloatDistinctReducer.
func NewFloatDistinctReducer() *FloatDistinctReducer {
	return &FloatDistinctReducer{m: make(map[float64]FloatPoint)}
}

// AggregateFloat aggregates a point into the reducer.
func (r *FloatDistinctReducer) AggregateFloat(p *FloatPoint) {
	if _, ok := r.m[p.Value]; !ok {
		r.m[p.Value] = *p
	}
}

// Emit emits the distinct points that have been aggregated into the reducer.
func (r *FloatDistinctReducer) Emit() []FloatPoint {
	points := make([]FloatPoint, 0, len(r.m))
	for _, p := range r.m {
		points = append(points, FloatPoint{Time: p.Time, Value: p.Value})
	}
	sort.Sort(floatPoints(points))
	return points
}

// FloatElapsedReducer calculates the elapsed of the aggregated points.
type FloatElapsedReducer struct {
	unitConversion int64
	prev           FloatPoint
	curr           FloatPoint
}

// NewFloatElapsedReducer creates a new FloatElapsedReducer.
func NewFloatElapsedReducer(interval Interval) *FloatElapsedReducer {
	return &FloatElapsedReducer{
		unitConversion: int64(interval.Duration),
		prev:           FloatPoint{Nil: true},
		curr:           FloatPoint{Nil: true},
	}
}

// AggregateFloat aggregates a point into the reducer and updates the current window.
func (r *FloatElapsedReducer) AggregateFloat(p *FloatPoint) {
	r.prev = r.curr
	r.curr = *p
}

// Emit emits the elapsed of the reducer at the current point.
func (r *FloatElapsedReducer) Emit() []IntegerPoint {
	if !r.prev.Nil {
		elapsed := (r.curr.Time - r.prev.Time) / r.unitConversion
		return []IntegerPoint{
			{Time: r.curr.Time, Value: elapsed},
		}
	}
	return nil
}

// IntegerPointAggregator aggregates points to produce a single point.
type IntegerPointAggregator interface {
	AggregateInteger(p *IntegerPoint)
}

// IntegerBulkPointAggregator aggregates multiple points at a time.
type IntegerBulkPointAggregator interface {
	AggregateIntegerBulk(points []IntegerPoint)
}

// AggregateIntegerPoints feeds a slice of IntegerPoint into an
// aggregator. If the aggregator is a IntegerBulkPointAggregator, it will
// use the AggregateBulk method.
func AggregateIntegerPoints(a IntegerPointAggregator, points []IntegerPoint) {
	switch a := a.(type) {
	case IntegerBulkPointAggregator:
		a.AggregateIntegerBulk(points)
	default:
		for _, p := range points {
			a.AggregateInteger(&p)
		}
	}
}

// IntegerPointEmitter produces a single point from an aggregate.
type IntegerPointEmitter interface {
	Emit() []IntegerPoint
}

// IntegerReduceFloatFunc is the function called by a IntegerPoint reducer.
type IntegerReduceFloatFunc func(prev *FloatPoint, curr *IntegerPoint) (t int64, v float64, aux []interface{})

type IntegerFuncFloatReducer struct {
	prev *FloatPoint
	fn   IntegerReduceFloatFunc
}

func NewIntegerFuncFloatReducer(fn IntegerReduceFloatFunc, prev *FloatPoint) *IntegerFuncFloatReducer {
	return &IntegerFuncFloatReducer{fn: fn, prev: prev}
}

func (r *IntegerFuncFloatReducer) AggregateInteger(p *IntegerPoint) {
	t, v, aux := r.fn(r.prev, p)
	if r.prev == nil {
		r.prev = &FloatPoint{}
	}
	r.prev.Time = t
	r.prev.Value = v
	r.prev.Aux = aux
	if p.Aggregated > 1 {
		r.prev.Aggregated += p.Aggregated
	} else {
		r.prev.Aggregated++
	}
}

func (r *IntegerFuncFloatReducer) Emit() []FloatPoint {
	return []FloatPoint{*r.prev}
}

// IntegerReduceFloatSliceFunc is the function called by a IntegerPoint reducer.
type IntegerReduceFloatSliceFunc func(a []IntegerPoint) []FloatPoint

type IntegerSliceFuncFloatReducer struct {
	points []IntegerPoint
	fn     IntegerReduceFloatSliceFunc
}

func NewIntegerSliceFuncFloatReducer(fn IntegerReduceFloatSliceFunc) *IntegerSliceFuncFloatReducer {
	return &IntegerSliceFuncFloatReducer{fn: fn}
}

func (r *IntegerSliceFuncFloatReducer) AggregateInteger(p *IntegerPoint) {
	r.points = append(r.points, *p)
}

func (r *IntegerSliceFuncFloatReducer) AggregateIntegerBulk(points []IntegerPoint) {
	r.points = append(r.points, points...)
}

func (r *IntegerSliceFuncFloatReducer) Emit() []FloatPoint {
	return r.fn(r.points)
}

// IntegerReduceFunc is the function called by a IntegerPoint reducer.
type IntegerReduceFunc func(prev *IntegerPoint, curr *IntegerPoint) (t int64, v int64, aux []interface{})

type IntegerFuncReducer struct {
	prev *IntegerPoint
	fn   IntegerReduceFunc
}

func NewIntegerFuncReducer(fn IntegerReduceFunc, prev *IntegerPoint) *IntegerFuncReducer {
	return &IntegerFuncReducer{fn: fn, prev: prev}
}

func (r *IntegerFuncReducer) AggregateInteger(p *IntegerPoint) {
	t, v, aux := r.fn(r.prev, p)
	if r.prev == nil {
		r.prev = &IntegerPoint{}
	}
	r.prev.Time = t
	r.prev.Value = v
	r.prev.Aux = aux
	if p.Aggregated > 1 {
		r.prev.Aggregated += p.Aggregated
	} else {
		r.prev.Aggregated++
	}
}

func (r *IntegerFuncReducer) Emit() []IntegerPoint {
	return []IntegerPoint{*r.prev}
}

// IntegerReduceSliceFunc is the function called by a IntegerPoint reducer.
type IntegerReduceSliceFunc func(a []IntegerPoint) []IntegerPoint

type IntegerSliceFuncReducer struct {
	points []IntegerPoint
	fn     IntegerReduceSliceFunc
}

func NewIntegerSliceFuncReducer(fn IntegerReduceSliceFunc) *IntegerSliceFuncReducer {
	return &IntegerSliceFuncReducer{fn: fn}
}

func (r *IntegerSliceFuncReducer) AggregateInteger(p *IntegerPoint) {
	r.points = append(r.points, *p)
}

func (r *IntegerSliceFuncReducer) AggregateIntegerBulk(points []IntegerPoint) {
	r.points = append(r.points, points...)
}

func (r *IntegerSliceFuncReducer) Emit() []IntegerPoint {
	return r.fn(r.points)
}

// IntegerReduceStringFunc is the function called by a IntegerPoint reducer.
type IntegerReduceStringFunc func(prev *StringPoint, curr *IntegerPoint) (t int64, v string, aux []interface{})

type IntegerFuncStringReducer struct {
	prev *StringPoint
	fn   IntegerReduceStringFunc
}

func NewIntegerFuncStringReducer(fn IntegerReduceStringFunc, prev *StringPoint) *IntegerFuncStringReducer {
	return &IntegerFuncStringReducer{fn: fn, prev: prev}
}

func (r *IntegerFuncStringReducer) AggregateInteger(p *IntegerPoint) {
	t, v, aux := r.fn(r.prev, p)
	if r.prev == nil {
		r.prev = &StringPoint{}
	}
	r.prev.Time = t
	r.prev.Value = v
	r.prev.Aux = aux
	if p.Aggregated > 1 {
		r.prev.Aggregated += p.Aggregated
	} else {
		r.prev.Aggregated++
	}
}

func (r *IntegerFuncStringReducer) Emit() []StringPoint {
	return []StringPoint{*r.prev}
}

// IntegerReduceStringSliceFunc is the function called by a IntegerPoint reducer.
type IntegerReduceStringSliceFunc func(a []IntegerPoint) []StringPoint

type IntegerSliceFuncStringReducer struct {
	points []IntegerPoint
	fn     IntegerReduceStringSliceFunc
}

func NewIntegerSliceFuncStringReducer(fn IntegerReduceStringSliceFunc) *IntegerSliceFuncStringReducer {
	return &IntegerSliceFuncStringReducer{fn: fn}
}

func (r *IntegerSliceFuncStringReducer) AggregateInteger(p *IntegerPoint) {
	r.points = append(r.points, *p)
}

func (r *IntegerSliceFuncStringReducer) AggregateIntegerBulk(points []IntegerPoint) {
	r.points = append(r.points, points...)
}

func (r *IntegerSliceFuncStringReducer) Emit() []StringPoint {
	return r.fn(r.points)
}

// IntegerReduceBooleanFunc is the function called by a IntegerPoint reducer.
type IntegerReduceBooleanFunc func(prev *BooleanPoint, curr *IntegerPoint) (t int64, v bool, aux []interface{})

type IntegerFuncBooleanReducer struct {
	prev *BooleanPoint
	fn   IntegerReduceBooleanFunc
}

func NewIntegerFuncBooleanReducer(fn IntegerReduceBooleanFunc, prev *BooleanPoint) *IntegerFuncBooleanReducer {
	return &IntegerFuncBooleanReducer{fn: fn, prev: prev}
}

func (r *IntegerFuncBooleanReducer) AggregateInteger(p *IntegerPoint) {
	t, v, aux := r.fn(r.prev, p)
	if r.prev == nil {
		r.prev = &BooleanPoint{}
	}
	r.prev.Time = t
	r.prev.Value = v
	r.prev.Aux = aux
	if p.Aggregated > 1 {
		r.prev.Aggregated += p.Aggregated
	} else {
		r.prev.Aggregated++
	}
}

func (r *IntegerFuncBooleanReducer) Emit() []BooleanPoint {
	return []BooleanPoint{*r.prev}
}

// IntegerReduceBooleanSliceFunc is the function called by a IntegerPoint reducer.
type IntegerReduceBooleanSliceFunc func(a []IntegerPoint) []BooleanPoint

type IntegerSliceFuncBooleanReducer struct {
	points []IntegerPoint
	fn     IntegerReduceBooleanSliceFunc
}

func NewIntegerSliceFuncBooleanReducer(fn IntegerReduceBooleanSliceFunc) *IntegerSliceFuncBooleanReducer {
	return &IntegerSliceFuncBooleanReducer{fn: fn}
}

func (r *IntegerSliceFuncBooleanReducer) AggregateInteger(p *IntegerPoint) {
	r.points = append(r.points, *p)
}

func (r *IntegerSliceFuncBooleanReducer) AggregateIntegerBulk(points []IntegerPoint) {
	r.points = append(r.points, points...)
}

func (r *IntegerSliceFuncBooleanReducer) Emit() []BooleanPoint {
	return r.fn(r.points)
}

// IntegerDistinctReducer returns the distinct points in a series.
type IntegerDistinctReducer struct {
	m map[int64]IntegerPoint
}

// NewIntegerDistinctReducer creates a new IntegerDistinctReducer.
func NewIntegerDistinctReducer() *IntegerDistinctReducer {
	return &IntegerDistinctReducer{m: make(map[int64]IntegerPoint)}
}

// AggregateInteger aggregates a point into the reducer.
func (r *IntegerDistinctReducer) AggregateInteger(p *IntegerPoint) {
	if _, ok := r.m[p.Value]; !ok {
		r.m[p.Value] = *p
	}
}

// Emit emits the distinct points that have been aggregated into the reducer.
func (r *IntegerDistinctReducer) Emit() []IntegerPoint {
	points := make([]IntegerPoint, 0, len(r.m))
	for _, p := range r.m {
		points = append(points, IntegerPoint{Time: p.Time, Value: p.Value})
	}
	sort.Sort(integerPoints(points))
	return points
}

// IntegerElapsedReducer calculates the elapsed of the aggregated points.
type IntegerElapsedReducer struct {
	unitConversion int64
	prev           IntegerPoint
	curr           IntegerPoint
}

// NewIntegerElapsedReducer creates a new IntegerElapsedReducer.
func NewIntegerElapsedReducer(interval Interval) *IntegerElapsedReducer {
	return &IntegerElapsedReducer{
		unitConversion: int64(interval.Duration),
		prev:           IntegerPoint{Nil: true},
		curr:           IntegerPoint{Nil: true},
	}
}

// AggregateInteger aggregates a point into the reducer and updates the current window.
func (r *IntegerElapsedReducer) AggregateInteger(p *IntegerPoint) {
	r.prev = r.curr
	r.curr = *p
}

// Emit emits the elapsed of the reducer at the current point.
func (r *IntegerElapsedReducer) Emit() []IntegerPoint {
	if !r.prev.Nil {
		elapsed := (r.curr.Time - r.prev.Time) / r.unitConversion
		return []IntegerPoint{
			{Time: r.curr.Time, Value: elapsed},
		}
	}
	return nil
}

// StringPointAggregator aggregates points to produce a single point.
type StringPointAggregator interface {
	AggregateString(p *StringPoint)
}

// StringBulkPointAggregator aggregates multiple points at a time.
type StringBulkPointAggregator interface {
	AggregateStringBulk(points []StringPoint)
}

// AggregateStringPoints feeds a slice of StringPoint into an
// aggregator. If the aggregator is a StringBulkPointAggregator, it will
// use the AggregateBulk method.
func AggregateStringPoints(a StringPointAggregator, points []StringPoint) {
	switch a := a.(type) {
	case StringBulkPointAggregator:
		a.AggregateStringBulk(points)
	default:
		for _, p := range points {
			a.AggregateString(&p)
		}
	}
}

// StringPointEmitter produces a single point from an aggregate.
type StringPointEmitter interface {
	Emit() []StringPoint
}

// StringReduceFloatFunc is the function called by a StringPoint reducer.
type StringReduceFloatFunc func(prev *FloatPoint, curr *StringPoint) (t int64, v float64, aux []interface{})

type StringFuncFloatReducer struct {
	prev *FloatPoint
	fn   StringReduceFloatFunc
}

func NewStringFuncFloatReducer(fn StringReduceFloatFunc, prev *FloatPoint) *StringFuncFloatReducer {
	return &StringFuncFloatReducer{fn: fn, prev: prev}
}

func (r *StringFuncFloatReducer) AggregateString(p *StringPoint) {
	t, v, aux := r.fn(r.prev, p)
	if r.prev == nil {
		r.prev = &FloatPoint{}
	}
	r.prev.Time = t
	r.prev.Value = v
	r.prev.Aux = aux
	if p.Aggregated > 1 {
		r.prev.Aggregated += p.Aggregated
	} else {
		r.prev.Aggregated++
	}
}

func (r *StringFuncFloatReducer) Emit() []FloatPoint {
	return []FloatPoint{*r.prev}
}

// StringReduceFloatSliceFunc is the function called by a StringPoint reducer.
type StringReduceFloatSliceFunc func(a []StringPoint) []FloatPoint

type StringSliceFuncFloatReducer struct {
	points []StringPoint
	fn     StringReduceFloatSliceFunc
}

func NewStringSliceFuncFloatReducer(fn StringReduceFloatSliceFunc) *StringSliceFuncFloatReducer {
	return &StringSliceFuncFloatReducer{fn: fn}
}

func (r *StringSliceFuncFloatReducer) AggregateString(p *StringPoint) {
	r.points = append(r.points, *p)
}

func (r *StringSliceFuncFloatReducer) AggregateStringBulk(points []StringPoint) {
	r.points = append(r.points, points...)
}

func (r *StringSliceFuncFloatReducer) Emit() []FloatPoint {
	return r.fn(r.points)
}

// StringReduceIntegerFunc is the function called by a StringPoint reducer.
type StringReduceIntegerFunc func(prev *IntegerPoint, curr *StringPoint) (t int64, v int64, aux []interface{})

type StringFuncIntegerReducer struct {
	prev *IntegerPoint
	fn   StringReduceIntegerFunc
}

func NewStringFuncIntegerReducer(fn StringReduceIntegerFunc, prev *IntegerPoint) *StringFuncIntegerReducer {
	return &StringFuncIntegerReducer{fn: fn, prev: prev}
}

func (r *StringFuncIntegerReducer) AggregateString(p *StringPoint) {
	t, v, aux := r.fn(r.prev, p)
	if r.prev == nil {
		r.prev = &IntegerPoint{}
	}
	r.prev.Time = t
	r.prev.Value = v
	r.prev.Aux = aux
	if p.Aggregated > 1 {
		r.prev.Aggregated += p.Aggregated
	} else {
		r.prev.Aggregated++
	}
}

func (r *StringFuncIntegerReducer) Emit() []IntegerPoint {
	return []IntegerPoint{*r.prev}
}

// StringReduceIntegerSliceFunc is the function called by a StringPoint reducer.
type StringReduceIntegerSliceFunc func(a []StringPoint) []IntegerPoint

type StringSliceFuncIntegerReducer struct {
	points []StringPoint
	fn     StringReduceIntegerSliceFunc
}

func NewStringSliceFuncIntegerReducer(fn StringReduceIntegerSliceFunc) *StringSliceFuncIntegerReducer {
	return &StringSliceFuncIntegerReducer{fn: fn}
}

func (r *StringSliceFuncIntegerReducer) AggregateString(p *StringPoint) {
	r.points = append(r.points, *p)
}

func (r *StringSliceFuncIntegerReducer) AggregateStringBulk(points []StringPoint) {
	r.points = append(r.points, points...)
}

func (r *StringSliceFuncIntegerReducer) Emit() []IntegerPoint {
	return r.fn(r.points)
}

// StringReduceFunc is the function called by a StringPoint reducer.
type StringReduceFunc func(prev *StringPoint, curr *StringPoint) (t int64, v string, aux []interface{})

type StringFuncReducer struct {
	prev *StringPoint
	fn   StringReduceFunc
}

func NewStringFuncReducer(fn StringReduceFunc, prev *StringPoint) *StringFuncReducer {
	return &StringFuncReducer{fn: fn, prev: prev}
}

func (r *StringFuncReducer) AggregateString(p *StringPoint) {
	t, v, aux := r.fn(r.prev, p)
	if r.prev == nil {
		r.prev = &StringPoint{}
	}
	r.prev.Time = t
	r.prev.Value = v
	r.prev.Aux = aux
	if p.Aggregated > 1 {
		r.prev.Aggregated += p.Aggregated
	} else {
		r.prev.Aggregated++
	}
}

func (r *StringFuncReducer) Emit() []StringPoint {
	return []StringPoint{*r.prev}
}

// StringReduceSliceFunc is the function called by a StringPoint reducer.
type StringReduceSliceFunc func(a []StringPoint) []StringPoint

type StringSliceFuncReducer struct {
	points []StringPoint
	fn     StringReduceSliceFunc
}

func NewStringSliceFuncReducer(fn StringReduceSliceFunc) *StringSliceFuncReducer {
	return &StringSliceFuncReducer{fn: fn}
}

func (r *StringSliceFuncReducer) AggregateString(p *StringPoint) {
	r.points = append(r.points, *p)
}

func (r *StringSliceFuncReducer) AggregateStringBulk(points []StringPoint) {
	r.points = append(r.points, points...)
}

func (r *StringSliceFuncReducer) Emit() []StringPoint {
	return r.fn(r.points)
}

// StringReduceBooleanFunc is the function called by a StringPoint reducer.
type StringReduceBooleanFunc func(prev *BooleanPoint, curr *StringPoint) (t int64, v bool, aux []interface{})

type StringFuncBooleanReducer struct {
	prev *BooleanPoint
	fn   StringReduceBooleanFunc
}

func NewStringFuncBooleanReducer(fn StringReduceBooleanFunc, prev *BooleanPoint) *StringFuncBooleanReducer {
	return &StringFuncBooleanReducer{fn: fn, prev: prev}
}

func (r *StringFuncBooleanReducer) AggregateString(p *StringPoint) {
	t, v, aux := r.fn(r.prev, p)
	if r.prev == nil {
		r.prev = &BooleanPoint{}
	}
	r.prev.Time = t
	r.prev.Value = v
	r.prev.Aux = aux
	if p.Aggregated > 1 {
		r.prev.Aggregated += p.Aggregated
	} else {
		r.prev.Aggregated++
	}
}

func (r *StringFuncBooleanReducer) Emit() []BooleanPoint {
	return []BooleanPoint{*r.prev}
}

// StringReduceBooleanSliceFunc is the function called by a StringPoint reducer.
type StringReduceBooleanSliceFunc func(a []StringPoint) []BooleanPoint

type StringSliceFuncBooleanReducer struct {
	points []StringPoint
	fn     StringReduceBooleanSliceFunc
}

func NewStringSliceFuncBooleanReducer(fn StringReduceBooleanSliceFunc) *StringSliceFuncBooleanReducer {
	return &StringSliceFuncBooleanReducer{fn: fn}
}

func (r *StringSliceFuncBooleanReducer) AggregateString(p *StringPoint) {
	r.points = append(r.points, *p)
}

func (r *StringSliceFuncBooleanReducer) AggregateStringBulk(points []StringPoint) {
	r.points = append(r.points, points...)
}

func (r *StringSliceFuncBooleanReducer) Emit() []BooleanPoint {
	return r.fn(r.points)
}

// StringDistinctReducer returns the distinct points in a series.
type StringDistinctReducer struct {
	m map[string]StringPoint
}

// NewStringDistinctReducer creates a new StringDistinctReducer.
func NewStringDistinctReducer() *StringDistinctReducer {
	return &StringDistinctReducer{m: make(map[string]StringPoint)}
}

// AggregateString aggregates a point into the reducer.
func (r *StringDistinctReducer) AggregateString(p *StringPoint) {
	if _, ok := r.m[p.Value]; !ok {
		r.m[p.Value] = *p
	}
}

// Emit emits the distinct points that have been aggregated into the reducer.
func (r *StringDistinctReducer) Emit() []StringPoint {
	points := make([]StringPoint, 0, len(r.m))
	for _, p := range r.m {
		points = append(points, StringPoint{Time: p.Time, Value: p.Value})
	}
	sort.Sort(stringPoints(points))
	return points
}

// StringElapsedReducer calculates the elapsed of the aggregated points.
type StringElapsedReducer struct {
	unitConversion int64
	prev           StringPoint
	curr           StringPoint
}

// NewStringElapsedReducer creates a new StringElapsedReducer.
func NewStringElapsedReducer(interval Interval) *StringElapsedReducer {
	return &StringElapsedReducer{
		unitConversion: int64(interval.Duration),
		prev:           StringPoint{Nil: true},
		curr:           StringPoint{Nil: true},
	}
}

// AggregateString aggregates a point into the reducer and updates the current window.
func (r *StringElapsedReducer) AggregateString(p *StringPoint) {
	r.prev = r.curr
	r.curr = *p
}

// Emit emits the elapsed of the reducer at the current point.
func (r *StringElapsedReducer) Emit() []IntegerPoint {
	if !r.prev.Nil {
		elapsed := (r.curr.Time - r.prev.Time) / r.unitConversion
		return []IntegerPoint{
			{Time: r.curr.Time, Value: elapsed},
		}
	}
	return nil
}

// BooleanPointAggregator aggregates points to produce a single point.
type BooleanPointAggregator interface {
	AggregateBoolean(p *BooleanPoint)
}

// BooleanBulkPointAggregator aggregates multiple points at a time.
type BooleanBulkPointAggregator interface {
	AggregateBooleanBulk(points []BooleanPoint)
}

// AggregateBooleanPoints feeds a slice of BooleanPoint into an
// aggregator. If the aggregator is a BooleanBulkPointAggregator, it will
// use the AggregateBulk method.
func AggregateBooleanPoints(a BooleanPointAggregator, points []BooleanPoint) {
	switch a := a.(type) {
	case BooleanBulkPointAggregator:
		a.AggregateBooleanBulk(points)
	default:
		for _, p := range points {
			a.AggregateBoolean(&p)
		}
	}
}

// BooleanPointEmitter produces a single point from an aggregate.
type BooleanPointEmitter interface {
	Emit() []BooleanPoint
}

// BooleanReduceFloatFunc is the function called by a BooleanPoint reducer.
type BooleanReduceFloatFunc func(prev *FloatPoint, curr *BooleanPoint) (t int64, v float64, aux []interface{})

type BooleanFuncFloatReducer struct {
	prev *FloatPoint
	fn   BooleanReduceFloatFunc
}

func NewBooleanFuncFloatReducer(fn BooleanReduceFloatFunc, prev *FloatPoint) *BooleanFuncFloatReducer {
	return &BooleanFuncFloatReducer{fn: fn, prev: prev}
}

func (r *BooleanFuncFloatReducer) AggregateBoolean(p *BooleanPoint) {
	t, v, aux := r.fn(r.prev, p)
	if r.prev == nil {
		r.prev = &FloatPoint{}
	}
	r.prev.Time = t
	r.prev.Value = v
	r.prev.Aux = aux
	if p.Aggregated > 1 {
		r.prev.Aggregated += p.Aggregated
	} else {
		r.prev.Aggregated++
	}
}

func (r *BooleanFuncFloatReducer) Emit() []FloatPoint {
	return []FloatPoint{*r.prev}
}

// BooleanReduceFloatSliceFunc is the function called by a BooleanPoint reducer.
type BooleanReduceFloatSliceFunc func(a []BooleanPoint) []FloatPoint

type BooleanSliceFuncFloatReducer struct {
	points []BooleanPoint
	fn     BooleanReduceFloatSliceFunc
}

func NewBooleanSliceFuncFloatReducer(fn BooleanReduceFloatSliceFunc) *BooleanSliceFuncFloatReducer {
	return &BooleanSliceFuncFloatReducer{fn: fn}
}

func (r *BooleanSliceFuncFloatReducer) AggregateBoolean(p *BooleanPoint) {
	r.points = append(r.points, *p)
}

func (r *BooleanSliceFuncFloatReducer) AggregateBooleanBulk(points []BooleanPoint) {
	r.points = append(r.points, points...)
}

func (r *BooleanSliceFuncFloatReducer) Emit() []FloatPoint {
	return r.fn(r.points)
}

// BooleanReduceIntegerFunc is the function called by a BooleanPoint reducer.
type BooleanReduceIntegerFunc func(prev *IntegerPoint, curr *BooleanPoint) (t int64, v int64, aux []interface{})

type BooleanFuncIntegerReducer struct {
	prev *IntegerPoint
	fn   BooleanReduceIntegerFunc
}

func NewBooleanFuncIntegerReducer(fn BooleanReduceIntegerFunc, prev *IntegerPoint) *BooleanFuncIntegerReducer {
	return &BooleanFuncIntegerReducer{fn: fn, prev: prev}
}

func (r *BooleanFuncIntegerReducer) AggregateBoolean(p *BooleanPoint) {
	t, v, aux := r.fn(r.prev, p)
	if r.prev == nil {
		r.prev = &IntegerPoint{}
	}
	r.prev.Time = t
	r.prev.Value = v
	r.prev.Aux = aux
	if p.Aggregated > 1 {
		r.prev.Aggregated += p.Aggregated
	} else {
		r.prev.Aggregated++
	}
}

func (r *BooleanFuncIntegerReducer) Emit() []IntegerPoint {
	return []IntegerPoint{*r.prev}
}

// BooleanReduceIntegerSliceFunc is the function called by a BooleanPoint reducer.
type BooleanReduceIntegerSliceFunc func(a []BooleanPoint) []IntegerPoint

type BooleanSliceFuncIntegerReducer struct {
	points []BooleanPoint
	fn     BooleanReduceIntegerSliceFunc
}

func NewBooleanSliceFuncIntegerReducer(fn BooleanReduceIntegerSliceFunc) *BooleanSliceFuncIntegerReducer {
	return &BooleanSliceFuncIntegerReducer{fn: fn}
}

func (r *BooleanSliceFuncIntegerReducer) AggregateBoolean(p *BooleanPoint) {
	r.points = append(r.points, *p)
}

func (r *BooleanSliceFuncIntegerReducer) AggregateBooleanBulk(points []BooleanPoint) {
	r.points = append(r.points, points...)
}

func (r *BooleanSliceFuncIntegerReducer) Emit() []IntegerPoint {
	return r.fn(r.points)
}

// BooleanReduceStringFunc is the function called by a BooleanPoint reducer.
type BooleanReduceStringFunc func(prev *StringPoint, curr *BooleanPoint) (t int64, v string, aux []interface{})

type BooleanFuncStringReducer struct {
	prev *StringPoint
	fn   BooleanReduceStringFunc
}

func NewBooleanFuncStringReducer(fn BooleanReduceStringFunc, prev *StringPoint) *BooleanFuncStringReducer {
	return &BooleanFuncStringReducer{fn: fn, prev: prev}
}

func (r *BooleanFuncStringReducer) AggregateBoolean(p *BooleanPoint) {
	t, v, aux := r.fn(r.prev, p)
	if r.prev == nil {
		r.prev = &StringPoint{}
	}
	r.prev.Time = t
	r.prev.Value = v
	r.prev.Aux = aux
	if p.Aggregated > 1 {
		r.prev.Aggregated += p.Aggregated
	} else {
		r.prev.Aggregated++
	}
}

func (r *BooleanFuncStringReducer) Emit() []StringPoint {
	return []StringPoint{*r.prev}
}

// BooleanReduceStringSliceFunc is the function called by a BooleanPoint reducer.
type BooleanReduceStringSliceFunc func(a []BooleanPoint) []StringPoint

type BooleanSliceFuncStringReducer struct {
	points []BooleanPoint
	fn     BooleanReduceStringSliceFunc
}

func NewBooleanSliceFuncStringReducer(fn BooleanReduceStringSliceFunc) *BooleanSliceFuncStringReducer {
	return &BooleanSliceFuncStringReducer{fn: fn}
}

func (r *BooleanSliceFuncStringReducer) AggregateBoolean(p *BooleanPoint) {
	r.points = append(r.points, *p)
}

func (r *BooleanSliceFuncStringReducer) AggregateBooleanBulk(points []BooleanPoint) {
	r.points = append(r.points, points...)
}

func (r *BooleanSliceFuncStringReducer) Emit() []StringPoint {
	return r.fn(r.points)
}

// BooleanReduceFunc is the function called by a BooleanPoint reducer.
type BooleanReduceFunc func(prev *BooleanPoint, curr *BooleanPoint) (t int64, v bool, aux []interface{})

type BooleanFuncReducer struct {
	prev *BooleanPoint
	fn   BooleanReduceFunc
}

func NewBooleanFuncReducer(fn BooleanReduceFunc, prev *BooleanPoint) *BooleanFuncReducer {
	return &BooleanFuncReducer{fn: fn, prev: prev}
}

func (r *BooleanFuncReducer) AggregateBoolean(p *BooleanPoint) {
	t, v, aux := r.fn(r.prev, p)
	if r.prev == nil {
		r.prev = &BooleanPoint{}
	}
	r.prev.Time = t
	r.prev.Value = v
	r.prev.Aux = aux
	if p.Aggregated > 1 {
		r.prev.Aggregated += p.Aggregated
	} else {
		r.prev.Aggregated++
	}
}

func (r *BooleanFuncReducer) Emit() []BooleanPoint {
	return []BooleanPoint{*r.prev}
}

// BooleanReduceSliceFunc is the function called by a BooleanPoint reducer.
type BooleanReduceSliceFunc func(a []BooleanPoint) []BooleanPoint

type BooleanSliceFuncReducer struct {
	points []BooleanPoint
	fn     BooleanReduceSliceFunc
}

func NewBooleanSliceFuncReducer(fn BooleanReduceSliceFunc) *BooleanSliceFuncReducer {
	return &BooleanSliceFuncReducer{fn: fn}
}

func (r *BooleanSliceFuncReducer) AggregateBoolean(p *BooleanPoint) {
	r.points = append(r.points, *p)
}

func (r *BooleanSliceFuncReducer) AggregateBooleanBulk(points []BooleanPoint) {
	r.points = append(r.points, points...)
}

func (r *BooleanSliceFuncReducer) Emit() []BooleanPoint {
	return r.fn(r.points)
}

// BooleanDistinctReducer returns the distinct points in a series.
type BooleanDistinctReducer struct {
	m map[bool]BooleanPoint
}

// NewBooleanDistinctReducer creates a new BooleanDistinctReducer.
func NewBooleanDistinctReducer() *BooleanDistinctReducer {
	return &BooleanDistinctReducer{m: make(map[bool]BooleanPoint)}
}

// AggregateBoolean aggregates a point into the reducer.
func (r *BooleanDistinctReducer) AggregateBoolean(p *BooleanPoint) {
	if _, ok := r.m[p.Value]; !ok {
		r.m[p.Value] = *p
	}
}

// Emit emits the distinct points that have been aggregated into the reducer.
func (r *BooleanDistinctReducer) Emit() []BooleanPoint {
	points := make([]BooleanPoint, 0, len(r.m))
	for _, p := range r.m {
		points = append(points, BooleanPoint{Time: p.Time, Value: p.Value})
	}
	sort.Sort(booleanPoints(points))
	return points
}

// BooleanElapsedReducer calculates the elapsed of the aggregated points.
type BooleanElapsedReducer struct {
	unitConversion int64
	prev           BooleanPoint
	curr           BooleanPoint
}

// NewBooleanElapsedReducer creates a new BooleanElapsedReducer.
func NewBooleanElapsedReducer(interval Interval) *BooleanElapsedReducer {
	return &BooleanElapsedReducer{
		unitConversion: int64(interval.Duration),
		prev:           BooleanPoint{Nil: true},
		curr:           BooleanPoint{Nil: true},
	}
}

// AggregateBoolean aggregates a point into the reducer and updates the current window.
func (r *BooleanElapsedReducer) AggregateBoolean(p *BooleanPoint) {
	r.prev = r.curr
	r.curr = *p
}

// Emit emits the elapsed of the reducer at the current point.
func (r *BooleanElapsedReducer) Emit() []IntegerPoint {
	if !r.prev.Nil {
		elapsed := (r.curr.Time - r.prev.Time) / r.unitConversion
		return []IntegerPoint{
			{Time: r.curr.Time, Value: elapsed},
		}
	}
	return nil
}
