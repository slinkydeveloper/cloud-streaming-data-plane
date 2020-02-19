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

cd example
compile_rust demo-join-function
build_image demo-join-function
compile_rust demo-logger
build_image demo-logger
