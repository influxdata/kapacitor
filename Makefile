VERSION := $(shell sh -c 'git describe --always --tags')
BRANCH:= $(shell sh -c 'git rev-parse --abbrev-ref HEAD')
COMMIT:= $(shell sh -c 'git rev-parse HEAD')
ifndef GOBIN
	GOBIN = $(GOPATH)/bin
endif
#GO=$(GOBIN)/godep go
GO=go

LDFLAGS="-X main.version=$(VERSION) -X main.branch=$(BRANCH) -X main.commit=$(COMMIT)"

build: prepare
	$(GO) build -o kapacitor -ldflags=$(LDFLAGS) \
		./cmd/kapacitor/main.go
	$(GO) build -o kapacitord -ldflags=$(LDFLAGS) \
		./cmd/kapacitord/main.go

build-linux-bins: prepare
	GOARCH=amd64 GOOS=linux $(GO) build -o kapacitor_linux_amd64 \
		-ldflags=$(LDFLAGS) \
		./cmd/kapacitor/main.go
	GOARCH=amd64 GOOS=linux $(GO) build -o kapacitord_linux_amd64 \
		-ldflags=$(LDFLAGS) \
		./cmd/kapacitord/main.go
	GOARCH=386 GOOS=linux $(GO) build -o kapacitor_linux_386 \
		-ldflags=$(LDFLAGS) \
		./cmd/kapacitor/main.go
	GOARCH=386 GOOS=linux $(GO) build -o kapacitord_linux_386 \
		-ldflags=$(LDFLAGS) \
		./cmd/kapacitord/main.go
	GOARCH=arm GOOS=linux $(GO) build -o kapacitor_linux_arm \
		-ldflags=$(LDFLAGS) \
		./cmd/kapacitor/main.go
	GOARCH=arm GOOS=linux $(GO) build -o kapacitord_linux_arm \
		-ldflags=$(LDFLAGS) \
		./cmd/kapacitord/main.go

prepare:
	go get -u -t ./...

test: prepare
	$(GO) tool vet --composites=false ./
	$(GO) test ./...

test-short: prepare
	$(GO) test -short ./...


.PHONY: test
