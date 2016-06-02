Go UUID implementation
========================

[![Build Status]()](https://ci.appveyor.com/api/projects/status/github/twinj/uuid?branch=master&svg=true)
[![Build Status](https://travis-ci.org/twinj/uuid.png?branch=master)](https://travis-ci.org/twinj/uuid)
[![GoDoc](http://godoc.org/github.com/twinj/uuid?status.png)](http://godoc.org/github.com/twinj/uuid)

This package provides RFC 4122 compliant UUIDs.
It will generate the following:

* Version 1: based on a Timestamp and MAC address as Node id
* Version 2: coming
* Version 3: based on MD5 hash
* Version 4: based on cryptographically secure random numbers
* Version 5: based on SHA-1 hash
* Your own implementations

Functions NewV1, NewV3, NewV4, NewV5, New, NewHex and Parse() for generating versions 1, 3, 4
and 5 UUIDs are as specified in [RFC 4122](http://www.ietf.org/rfc/rfc4122.txt).

# Requirements

Go 1.6, 1.5, 1.4, 1.3, 1.2.

# Design considerations

* Ensure UUIDs are unique across a use case
    Proper test coverage has determined that the UUID timestamp spinner works correctly
* the generator should work on all app servers.
    No Os locking threads or file system dependant storage.
    Saver interface exists for the user to provide their own Saver implementations
    for V1 and V2 UUIDs. The interface could theoretically be applied to your own UUID implementation.
    Have provided a saver which works on a standard OS environment.
    New implementations for app servers which stop
* UUIDs

# Future considerations

* allows open ended user implementations
* allow open ended generation of UUID via various node id's

# Recent Changes

* Improved file system Saver interface, breaking changes, however.
    To use a saver make sure you pass it in via the uuid.SetupSaver(Saver) method before a UUID is generated, so as to take affect.
* Removed use of OS Thread locking and runtime package requirement
* Changed String() output to CleanHyphen to match the canonical standard
* Removed default non volatile store and replaced with Saver interface
* Added formatting support for user defined formats
* Added support for Google App Engine
* Variant type bits are now set correctly
* Variant type can now be retrieved more efficiently
* New tests for variant setting to confirm correctness
* New tests added to confirm proper version setting

## Installation

Use the `go` tool:

	$ go get github.com/twinj/uuid

## Usage

See [documentation and examples](http://godoc.org/github.com/twinj/uuid)
for more information.

	uuid.SetupSaver(config)

	u1 := uuid.NewV1()

	uP, _ := uuid.Parse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

	u3 := uuid.NewV3(uP, uuid.Name("test"))
	u4 := uuid.NewV4()
	fmt.Printf(print, u4.Version(), u4.Variant(), u4)

	u5 := uuid.NewV5(uuid.NamespaceURL, uuid.Name("test"))

	if uuid.Equal(u1, u3) {
		fmt.Printf("Will never happen")
	}
	fmt.Printf(uuid.Formatter(u5, uuid.CurlyHyphen))

	uuid.SwitchFormat(uuid.BracketHyphen)

## Copyright

This is a derivative work

Orginal version from
Copyright (C) 2011 by Krzysztof Kowalik <chris@nu7hat.ch>.
See [COPYING](https://github.com/nu7hatch/gouuid/tree/master/COPYING)
file for details.

Also see: Algorithm details in [RFC 4122](http://www.ietf.org/rfc/rfc4122.txt).

Copyright (C) 2014 twinj@github.com
See [LICENSE](https://github.com/twinj/uuid/tree/master/LICENSE)
file for details.
