#!/usr/bin/env bash

PWD=$(pwd)
COVFILE=${PWD}/coverage.txt

function test_and_append_coverage() {
	pushd $1 > /dev/null
	go test -v -coverprofile=profile.out -covermode=atomic
	if [ -f profile.out ]; then
		cat profile.out >> ${COVFILE}
		rm profile.out
	fi
	popd > /dev/null
}

set -e
echo "" > ${COVFILE}

test_and_append_coverage src/search
