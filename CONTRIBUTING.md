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
Kapacitor requires Go 1.5 or greater.

At Kapacitor we find gvm, a Go version manager, useful for installing Go. For instructions
on how to install it see [the gvm page on github](https://github.com/moovweb/gvm).

After installing gvm you can install and set the default go version by
running the following:

    gvm install go1.5
    gvm use go1.5 --default

Revision Control Systems
------------------------
Go has the ability to import remote packages via revision control systems with the `go get` command.  To ensure that you can retrieve any remote package, be sure to install the following rcs software to your system.
Currently the project only depends on `git` and `mercurial`.

* [Install Git](http://git-scm.com/book/en/Getting-Started-Installing-Git)
* [Install Mercurial](http://mercurial.selenic.com/wiki/Download)

Getting the source
------
Setup the project structure and fetch the repo like so:

    mkdir $HOME/gocodez
    export GOPATH=$HOME/gocodez
    go get github.com/influxdb/kapacitor

You can add the line `export GOPATH=$HOME/gocodez` to your bash/zsh file to be set for every shell instead of having to manually run it everytime.

Cloning a fork
-------------
If you wish to work with fork of Kapacitor, your own fork for example, you must still follow the directory structure above.
But instead of cloning the main repo, instead clone your fork. Follow the steps below to work with a fork:

    export GOPATH=$HOME/gocodez
    mkdir -p $GOPATH/src/github.com/influxdb
    cd $GOPATH/src/github.com/influxdb
    git clone git@github.com:<username>/kapacitor

Retaining the directory structure `$GOPATH/src/github.com/influxdb` is necessary so that Go imports work correctly.

Pre-commit checks
-------------

We have a pre-commit hook to make sure code is formatted properly and vetted before you commit any changes. We strongly recommend using the pre-commit hook to guard against accidentally committing unformatted code. To use the pre-commit hook, run the following:

    cd $GOPATH/src/github.com/influxdb/kapacitor
    cp .hooks/pre-commit .git/hooks/

In case the commit is rejected because it's not formatted you can run
the following to format the code:

```
go fmt ./...
go vet ./...
```

To install go vet, run the following command:
```
go get golang.org/x/tools/cmd/vet
```

NOTE: If you have not installed mercurial, the above command will fail.  See [Revision Control Systems](#revision-control-systems) above.

For more information on `go vet`, [read the GoDoc](https://godoc.org/golang.org/x/tools/cmd/vet).

Build and Test
--------------

Make sure you have Go installed and the project structure as shown above. To then build the project, execute the following commands:

```bash
cd $GOPATH/src/github.com/influxdb/kapacitor
go build ./cmd/kapacitor
go build ./cmd/kapacitord
```
Kapacitor builds two binares is named `kapacitor`, and `kapacitord`.

To run the tests, execute the following command:

```bash
go test ./...
```

If you want to build packages run:
```bash
./build.py --packages
```


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
