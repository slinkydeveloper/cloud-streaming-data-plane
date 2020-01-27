#!/bin/bash

function compile_rust_function() {
  cd "./$1"
  cargo build --target x86_64-unknown-linux-musl --release
  cd ..
}

function build_image() {
  cd "./$1"
  docker build -t "slinkydeveloper/$1" .
  cd ..
}

mvn package
compile_rust_function demo-join-function

eval $(minikube docker-env)
build_image demo-join-function
build_image inbound
build_image runtime
