#!/usr/bin/env bash

# start cluster
minikube config set cpus 4
minikube config set memory 8Gi
minikube start

# create spark namespace
kubectl apply -f namespace.yaml

