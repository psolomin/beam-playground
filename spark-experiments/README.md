# Spark Experiments

Run tests

```
./gradlew test
```

Start database

```
docker-compose up --build -d db
```

Run tests

```
./gradlew test
```

Run app

```
./gradlew run --args="100"
```

Check what was inserted

```
mysql --host=127.0.0.1 --port=3306 \
 --database=my_db --user=my --password=my
```

## Minikube

Requires:
- kubectl 1.22.2 or higher
- minikube 1.29.0 or higher

If you need to generate your own images and want to use Spark distro tooling:

```
mkdir -p distros

curl https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz \
    -o ./distros/spark-3.3.2-bin-hadoop3.tgz

tar zxvf ./distros/spark-3.3.2-bin-hadoop3.tgz --directory ./distros

./distros/spark-3.3.2-bin-hadoop3/bin/docker-image-tool.sh \
    -r my-spark.foo/my-spark -t 3.3.2-java11 build

minikube image load my-spark.foo/my-spark/spark:3.3.2-java11 --overwrite=true

```

When image is loaded, follow the scripts under [k8s dir](./k8s).

Once finished, you can clean up via:

```
minikube delete --all --purge
```


Caused by: javax.net.ssl.SSLPeerUnverifiedException: Hostname minikube not verified:
certificate: sha256/+E7cgJM8Kc38pbSt3ot6BZl2R0qj8Qi668pNGq2s/Jo=
DN: CN=minikube, O=system:masters
subjectAltNames: [192.168.49.2, 10.96.0.1, 127.0.0.1, 10.0.0.1, minikubeCA, control-plane.minikube.internal, kubernetes.default.svc.cluster.local, kubernetes.default.svc, kubernetes.default, kubernetes, localhost]
