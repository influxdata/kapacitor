#!/bin/bash

refactor_from() {
    grep -rl "[^e]StreamNode" --include '*.go' . | while read f; do sed -i.bak "s/\([^e]\)StreamNode/\1FromNode/g" $f; done
}

refactor_stream() {
    grep -rl "SourceStreamNode" --include '*.go' . | while read f; do sed -i.bak "s/SourceStreamNode/StreamNode/g" $f; done
}

refactor_query() {
    grep -rl "[^e]BatchNode" --include '*.go' . | while read f; do sed -i.bak "s/\([^e]\)BatchNode/\1QueryNode/g" $f; done
}

refactor_batch() {
    grep -rl "SourceBatchNode" --include '*.go' . | while read f; do sed -i.bak "s/SourceBatchNode/BatchNode/g" $f; done
}

refactor_once() {
	local proc=$1
	shift 1

	$proc &&
	git clean -fd &&
	go build cmd/kapacitord/main.go &&
	git diff --name-only | xargs -n1 git add &&
	git commit -m "$*

	Automatic refactoring performed by refactor-doc.sh
	" || exit 1
}

refactor_once refactor_from "Rename StreamNode to FromNode"
refactor_once refactor_stream "Rename SourceStreamNode to StreamNode"
refactor_once refactor_query "Rename BatchNode to QueryNode"
refactor_once refactor_batch "Rename SourceBatchNode to BatchNode"

git rm refactor-doc.sh &&
git commit -m "Remove refactor-doc.sh"
