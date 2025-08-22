#!/bin/bash -e

export CARGO_TERM_PROGRESS_WHEN=always
export GOFLAGS="-x -v"

go install google.golang.org/protobuf/cmd/protoc-gen-go
go install github.com/benbjohnson/tmpl
go install github.com/mailru/easyjson/easyjson

function check_changes () {
  changes="$(git status --porcelain=v1 2>/dev/null)"
  if [ -n "$changes" ] ; then
    echo $1
    echo "$changes"
    git --no-pager diff
    exit 1
  fi
}

check_changes "git is dirty before running generate!"

go generate ./...

check_changes "git is dirty after running generate!"
