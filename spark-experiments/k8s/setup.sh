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
  --from-file=exec_tpl=./executor-template.yaml

# create client pod - apps will be submitted from it in local or cluster mode:
kubectl apply -f spark-shell-pod.yaml

kubectl -n my-spark exec -it spark-client -- sh -c 'cd /opt/spark/; ./bin/spark-shell \
    --master k8s://https://kubernetes.default.svc:443 \
    --deploy-mode client \
    --conf spark.kubernetes.namespace=my-spark \
    --conf spark.kubernetes.container.image=my-spark.foo/my-spark/spark:3.3.2-java11 \
    --conf spark.kubernetes.container.image.pullPolicy=Never  \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=my-spark-sa \
    --conf spark.kubernetes.authenticate.serviceAccountName=my-spark-sa \
    --conf spark.kubernetes.driver.pod.name=spark-client \
    --conf spark.kubernetes.executor.podTemplateFile=file:///etc/spark/executor-template.yaml \
    --conf spark.executor.instances=1 \
    --conf spark.executor.memory=1G \
    --conf spark.driver.memory=512M \
    --conf spark.driver.host=spark-client-headless \
    --conf spark.driver.port=19987 \
    --conf spark.jars.ivy=/tmp/.ivy'
