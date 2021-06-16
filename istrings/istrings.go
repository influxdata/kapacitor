package istrings

// Copyright 2021 Influxdata. All rights reserved. This is very heavily based on code that was:
//
// Copyright 2020 Brad Fitzpatrick. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package intern lets you make smaller comparable strings by boxing
// a larger comparable string down into a globally unique 8 byte pointer.
// The globally unique pointers are garbage collected with weak
// references and finalizers. This package hides that.
//
// The GitHub repo this code is based on is https://github.com/go4org/intern

import (
	"encoding/json"
	"runtime"
	"sort"
	"sync"
	"unsafe"

	"github.com/zeebo/xxh3"
	_ "go4.org/unsafe/assume-no-moving-gc"
)

const istringMutexLen = 1 << 8 // must be a power of 2

var (
	// mu guards valMap, a weakref map of *IString by underlying value.
	// It also guards the resurrected field of all *Values.
	mu [istringMutexLen]sync.Mutex

	// valMap is the global string intern, we init it directly here via a lambda instead of
	// using init, so it is avaliable earlier in the startup process
	valMap = func() [istringMutexLen]map[string]uintptr {
		x := [istringMutexLen]map[string]uintptr{}
		for k := range x {
			x[k] = map[string]uintptr{}
		}
		return x
	}()
)

// A IString is the handle to an underlying comparable value.
// See func Get for how IString pointers may be used.
type IString struct {
	*value
}

type value struct {
	_ [0]func() // prevent people from accidentally using value type as comparable
	s string
	// resurrected is guarded by mu (for all instances of IString).
	// It is set true whenever v is synthesized from a uintptr.
	resurrected bool
}

// Len returns true iff s is an empty string (or nil).
func (s *value) Len() int {
	if s == nil {
		return 0
	}
	return len(s.s)
}

// reset resets and initalizes the valMap
func reset() {
	for i := 0; i < istringMutexLen; i++ {
		mu[i].Lock()
		valMap[i] = map[string]uintptr{}
		mu[i].Unlock()
	}
}

// initalize the
func init() {
	reset()
}

func (v value) mID() int {
	return int(xxh3.HashString(v.s) & (istringMutexLen - 1))
}

// Get returns the comparable value passed to the Get func
// that returned v.
func (v *value) String() string {
	if v == nil {
		return ""
	}
	return v.s
}

func (v *IString) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	v.value = get(s)
	return nil
}

// Get  is specialized for strings.
// It avoids an allocation from putting a string into an interface{}
// to pass as an argument to Get.
func Get(k string) IString {
	return IString{get(k)}
}

// We play unsafe games that violate Go's rules (and assume a non-moving
// collector). So we quiet Go here.
// See the comment below Get for more implementation details.
//go:nocheckptr
func get(k string) *value {
	if k == "" {
		return nil
	}
	mID := xxh3.HashString(k) & (istringMutexLen - 1)
	mu[mID].Lock()
	defer mu[mID].Unlock()

	localMap := valMap[mID]
	var v *value
	if addr, ok := localMap[k]; ok {
		v = (*value)((unsafe.Pointer)(addr))
		v.resurrected = true
		return v
	}
	v = &value{s: k}
	// SetFinalizer before uintptr conversion (theoretical concern;
	// see https://github.com/go4org/intern/issues/13)
	runtime.SetFinalizer(v, finalize)
	localMap[k] = uintptr(unsafe.Pointer(v))
	return v
}

func finalize(v *value) {
	mID := v.mID()
	mu[mID].Lock()
	defer mu[mID].Unlock()
	if v.resurrected {
		// We lost the race. Somebody resurrected it while we
		// were about to finalize it. Try again next round.
		v.resurrected = false
		runtime.SetFinalizer(v, finalize)
		return
	}
	delete(valMap[mID], v.String())
}

func Sort(s []IString) {
	sort.Sort(IStringSlice(s))
}

// IStringSlice attaches the methods of Interface to []iString, sorting in increasing order.
type IStringSlice []IString

func (x IStringSlice) Len() int           { return len(x) }
func (x IStringSlice) Less(i, j int) bool { return x[i].String() < x[j].String() }
func (x IStringSlice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// Sort is a convenience method: x.Sort() calls Sort(x).
func (x IStringSlice) Sort() { Sort(x) }

// Interning is simple if you don't require that unused values be
// garbage collectable. But we do require that; we don't want to be
// DOS vector. We do this by using a uintptr to hide the pointer from
// the garbage collector, and using a finalizer to eliminate the
// pointer when no other code is using it.
//
// The obvious implementation of this is to use a
// map[interface{}]uintptr-of-*interface{}, and set up a finalizer to
// delete from the map. Unfortunately, this is racy. Because pointers
// are being created in violation of Go's unsafety rules, it's
// possible to create a pointer to a value concurrently with the GC
// concluding that the value can be collected. There are other races
// that break the equality invariant as well, but the use-after-free
// will cause a runtime crash.
//
// To make this work, the finalizer needs to know that no references
// have been unsafely created since the finalizer was set up. To do
// this, values carry a "resurrected" sentinel, which gets set
// whenever a pointer is unsafely created. If the finalizer encounters
// the sentinel, it clears the sentinel and delays collection for one
// additional GC cycle, by re-installing itself as finalizer. This
// ensures that the unsafely created pointer is visible to the GC, and
// will correctly prevent collection.
//
// This technique does mean that interned values that get reused take
// at least 3 GC cycles to fully collect (1 to clear the sentinel, 1
// to clean up the unsafe map, 1 to be actually deleted).
//
// @ianlancetaylor commented in
// https://github.com/golang/go/issues/41303#issuecomment-717401656
// that it is possible to implement weak references in terms of
// finalizers without unsafe. Unfortunately, the approach he outlined
// does not work here, for two reasons. First, there is no way to
// construct a strong pointer out of a weak pointer; our map stores
// weak pointers, but we must return strong pointers to callers.
// Second, and more fundamentally, we must return not just _a_ strong
// pointer to callers, but _the same_ strong pointer to callers. In
// order to return _the same_ strong pointer to callers, we must track
// it, which is exactly what we cannot do with strong pointers.
//
// See https://github.com/inetaf/netaddr/issues/53 for more
// discussion, and https://github.com/go4org/intern/issues/2 for an
// illustration of the subtleties at play.
