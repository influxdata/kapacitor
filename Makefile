SHORT_VERSION := $(shell sh -c 'git describe --always --tags --abbrev=0')
COUNT := $(shell sh -c 'git rev-list $(SHORT_VERSION)..HEAD --count')
VERSION=$(SHORT_VERSION).$(COUNT)
BRANCH:= $(shell sh -c 'git rev-parse --abbrev-ref HEAD')
COMMIT:= $(shell sh -c 'git rev-parse HEAD')
ifndef GOBIN
	GOBIN = $(GOPATH)/bin
endif
GO=go
DIST_DIR=./dist
# List of Golang os/arch pairs
OS_ARCH=linux/amd64 darwin/amd64 windows/amd64

LDFLAGS=-X main.version=$(VERSION) -X main.branch=$(BRANCH) -X main.commit=$(COMMIT)

build: prepare
	$(GO) build -o kapacitor -ldflags=$(LDFLAGS) \
		./cmd/kapacitor/main.go
	$(GO) build -o kapacitord -ldflags=$(LDFLAGS) \
		./cmd/kapacitord/main.go

dist: prepare
	rm -rf $(DIST_DIR)
	mkdir $(DIST_DIR)
	gox -ldflags="$(LDFLAGS)" -osarch="$(OS_ARCH)" -output "$(DIST_DIR)/{{.Dir}}_$(VERSION)_{{.OS}}_{{.Arch}}" ./cmd/kapacitor ./cmd/kapacitord

release: dist
	# This command requires that GITHUB_TOKEN env be set with a token that can create releases.
	ghr -u influxdb -r kapacitor $(SHORT_VERSION) dist/

update:
	$(GO) get -u -t ./...

prepare:
	$(GO) get -t ./...
	$(GO) get github.com/mitchellh/gox
	$(GO) get github.com/tcnksm/ghr

test: prepare
	$(GO) tool vet --composites=false ./
	$(GO) test ./...

test-short: prepare
	$(GO) test -short ./...


.PHONY: test
