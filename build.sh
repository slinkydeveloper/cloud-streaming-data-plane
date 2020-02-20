#!/bin/bash

set -e

function compile_rust() {
  cd "./$1"
  RUSTC_WRAPPER=sccache cargo build --target x86_64-unknown-linux-musl --release
  cd ..
}

function build_image() {
  cd "./$1"
  docker build -t "slinkydeveloper/$1" .
  cd ..
}

eval $(minikube docker-env)
mvn package jib:dockerBuild

cd example-sum-flow
compile_rust demo-join-function
build_image demo-join-function
compile_rust demo-logger
build_image demo-logger

cd ..
cd example-stock-flow
compile_rust demo-stocks-filter
build_image demo-stocks-filter
compile_rust demo-stocks-decisions
build_image demo-stocks-decisions
compile_rust demo-stocks-decisions-logger
build_image demo-stocks-decisions-logger
