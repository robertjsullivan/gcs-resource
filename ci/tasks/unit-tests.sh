#!/bin/bash

set -e

export GOPATH=$PWD/go
export PATH=$GOPATH/bin:$PATH
export CGO_ENABLED=1

cd $GOPATH/src/github.com/frodenas/gcs-resource
make
