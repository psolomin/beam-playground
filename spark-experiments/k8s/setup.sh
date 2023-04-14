#!/usr/bin/env bash

# Adopted from https://timothyzhang.medium.com/running-spark-shell-on-kubernetes-3181d4446622

# start cluster
minikube config set cpus 4
minikube config set memory 8G
minikube start --insecure-registry "10.0.0.0/24"
minikube addons enable ingress
minikube addons enable ingress-dns

# create spark namespace:
kubectl apply -f namespace.yaml

# load local config files into mountable volumes:
kubectl -n my-spark create configmap configs \
  --from-file=exec_tpl=./executor-template.yaml \
  --save-config -o yaml --dry-run=client \
  | kubectl apply -f -

# create client pod - apps will be submitted from it in local or cluster mode:
kubectl apply -f spark-shell-pod.yaml

# create a database:
kubectl apply -f mysql.yaml
