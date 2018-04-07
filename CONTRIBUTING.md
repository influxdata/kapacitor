Contributing to Kapacitor
=========================

Bug reports
---------------
Before you file an issue, please search existing issues in case it has already been filed, or perhaps even fixed.
If you file an issue, please include the following.
* Full details of your operating system (or distribution) e.g. 64-bit Ubuntu 14.04.
* The version of Kapacitor you are running
* Whether you installed it using a pre-built package, or built it from source.
* A small test case, if applicable, that demonstrates the issues.

Remember the golden rule of bug reports: **The easier you make it for us to reproduce the problem, the faster it will get fixed.**
If you have never written a bug report before, or if you want to brush up on your bug reporting skills, we recommend reading [Simon Tatham's essay "How to Report Bugs Effectively."](http://www.chiark.greenend.org.uk/~sgtatham/bugs.html)

Please note that issues are *not the place to file general questions* such as "how do I use InfluxDB with Kapacitor?" Questions of this nature should be sent to the [Google Group](https://groups.google.com/forum/#!forum/influxdb), not filed as issues. Issues like this will be closed.

Feature requests
---------------
We really like to receive feature requests, as it helps us prioritize our work.
Please be clear about your requirements, as incomplete feature requests may simply be closed if we don't understand what you would like to see added to Kapacitor.

Contributing to the source code
---------------

Kapacitor follows standard Go project structure.
This means that all your go development are done in `$GOPATH/src`.
GOPATH can be any directory under which InfluxDB and all its dependencies will be cloned.
For more details on recommended go project's structure, see [How to Write Go Code](http://golang.org/doc/code.html) and
[Go: Best Practices for Production Environments](http://peter.bourgon.org/go-in-production/), or you can just follow the steps below.

Submitting a pull request
------------
To submit a pull request you should fork the Kapacitor repository, and make your change on a feature branch of your fork.
Then generate a pull request from your branch against *master* of the Kapacitor repository.
Include in your pull request details of your change -- the why *and* the how -- as well as the testing your performed.
Also, be sure to run the test suite with your change in place. Changes that cause tests to fail cannot be merged.

There will usually be some back and forth as we finalize the change, but once that completes it may be merged.

To assist in review for the PR, please add the following to your pull request comment:

```md
- [ ] CHANGELOG.md updated
- [ ] Rebased/mergable
- [ ] Tests pass
- [ ] Sign [CLA](http://influxdb.com/community/cla.html) (if not already signed)
```

Use of third-party packages
---------------------------
A third-party package is defined as one that is not part of the standard Go distribution.
Generally speaking we prefer to minimize our use of third-party packages, and avoid them unless absolutely necessarily.
We'll often write a little bit of code rather than pull in a third-party package.
So to maximise the chance your change will be accepted by us, use only the standard libraries, or the third-party packages we have decided to use.

For rationale, check out the post [The Case Against Third Party Libraries](http://blog.gopheracademy.com/advent-2014/case-against-3pl/).

Signing the CLA
---------------

If you are going to be contributing back to Kapacitor please take a second to sign our CLA, which can be found
[on our website](http://influxdb.com/community/cla.html).

Installing Go
-------------

Kapacitor typically requires the lastest version of Go.

To install go see https://golang.org/dl/

Getting the source
------
Setup the project structure and fetch the repo like so:

    mkdir $HOME/go
    export GOPATH=$HOME/go
    go get github.com/influxdata/kapacitor

You can add the line `export GOPATH=$HOME/go` to your bash/zsh file to be set for every shell instead of having to manually run it everytime.

Cloning a fork
-------------
If you wish to work with fork of Kapacitor, your own fork for example, you must still follow the directory structure above.
But instead of cloning the main repo, instead clone your fork. Follow the steps below to work with a fork:

    export GOPATH=$HOME/go
    mkdir -p $GOPATH/src/github.com/influxdata
    cd $GOPATH/src/github.com/influxdata
    git clone git@github.com:<username>/kapacitor.git

Retaining the directory structure `$GOPATH/src/github.com/influxdata` is necessary so that Go imports work correctly.

Pre-commit checks
-------------

We have a pre-commit hook to make sure code is formatted properly and vetted before you commit any changes. We strongly recommend using the pre-commit hook to guard against accidentally committing unformatted code. To use the pre-commit hook, run the following:

    cd $GOPATH/src/github.com/influxdata/kapacitor
    cp .hooks/pre-commit .git/hooks/

In case the commit is rejected because it's not formatted you can run
the following to format the code:

```
go fmt ./...
go vet ./...
```

For more information on `go vet`, [read the GoDoc](https://golang.org/pkg/cmd/go/internal/vet/).

Build and Test
--------------

Make sure you have Go installed and the project structure as shown above. To then build the project, execute the following commands:

```bash
cd $GOPATH/src/github.com/influxdata/kapacitor
go build ./cmd/kapacitor
go build ./cmd/kapacitord
```
Kapacitor builds two binares is named `kapacitor`, and `kapacitord`.

To run the tests, execute the following command:

```bash
go test $(go list ./... | grep -v /vendor/)
```

Dependencies
------------

Kapacitor vendors all dependencies.
Kapacitor uses the golang [dep](https://github.com/golang/dep) tool.

Install the dep tool:

```
go get -v -u github.com/golang/dep/cmd/dep
```

See the dep help for usage and documentation.

Kapacitor commits vendored deps into the repo, as a result always run `dep prune` after any `dep ensure` operation.
This helps keep the amount of code committed to a minimum.


Generating Code
---------------

Kapacitor uses generated code.
The generated code is committed to the repository so normally it is not necessary to regenerate it.
But if you modify one of the templates for code generation you must re-run the generate commands.

Go provides a consistent command for generating all necessary code:

```bash
go generate ./...
```

For the generate command to succeed you will need a few dependencies installed on your system.
These dependencies are already vendored in the code and and can be installed from there.

* tmpl -- A utility used to generate code from templates. Install via `go install ./vendor/github.com/benbjohnson/tmpl`
* protoc + protoc-gen-go -- A protobuf compiler plus the protoc-gen-go extension.
    You need version 3.0.0 of protoc.
    To install the go plugin run `go install ./vendor/github.com/golang/protobuf/protoc-gen-go`

The Build Script
----------------

The above commands have all be encapsulated for you in a `build.py` script.
The script has flags for testing code, building binaries and complete distribution packages.

To build kapacitor use:

```bash
./build.py
```

To run the tests use:

```bash
./build.py --test
```

If you want to generate code run:

```bash
./build.py --generate
```

If you want to build packages run:

```bash
./build.py --package
```

There are many more options available see

```bash
./build.py --help
```


The Build Script + Docker
-------------------------

Kapacitor requires a few extra dependencies to perform certain build actions.
Specifically to build packages or to regenerate any of the generated code you will need a few extra tools.
A `build.sh` script is provided that will run `build.py` in a docker container with all the needed dependencies installed with correct versions.

All you need is to have docker installed and then use the `./build.sh` command as if it were the `./build.py` command.


Profiling
---------
When troubleshooting problems with CPU or memory the Go toolchain can be helpful. You can start InfluxDB with CPU or memory profiling turned on. For example:

```sh
# start kapacitord with profiling
./kapacitord -cpuprofile kapacitord.prof
# run task, replays whatever you're testing
# Quit out of kapacitord and kapacitord.prof will then be written.
# open up pprof to examine the profiling data.
go tool pprof ./kapacitord kapacitord.prof
# once inside run "web", opens up browser with the CPU graph
# can also run "web <function name>" to zoom in. Or "list <function name>" to see specific lines
```
Note that when you pass the binary to `go tool pprof` *you must specify the path to the binary*.

Continuous Integration testing
------------------------------
Kapacitor uses CircleCI for continuous integration testing.

Useful links
------------
- [Useful techniques in Go](http://arslan.io/ten-useful-techniques-in-go)
- [Go in production](http://peter.bourgon.org/go-in-production/)
- [Principles of designing Go APIs with channels](https://inconshreveable.com/07-08-2014/principles-of-designing-go-apis-with-channels/)
- [Common mistakes in Golang](http://soryy.com/blog/2014/common-mistakes-with-go-lang/). Especially this section `Loops, Closures, and Local Variables`
