#!/bin/bash

function build_image() {
  cd "./$1"
  docker build -t "slinkydeveloper/$1" .
  cd ..
}

mvn package

eval $(minikube docker-env)
build_image demo-join-function
build_image inbound
build_image join
