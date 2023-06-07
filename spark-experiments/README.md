# Spark Experiments

Requirements:

- Java 11 or higher


Build and test

```
./gradlew clean build
```

Start database

```
docker-compose up --build -d db
```

Download Spark distro

```
mkdir -p distros

curl https://dlcdn.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz \
    -o ./distros/spark-3.4.0-bin-hadoop3.tgz

tar zxvf ./distros/spark-3.4.0-bin-hadoop3.tgz --directory ./distros

```

Submit spark job:

```
distros_dir=./distros/spark-3.4.0-bin-hadoop3

./gradlew clean build copyAllDependencies

${distros_dir}/bin/spark-submit \
    --conf spark.driver.extraClassPath="./build/user-libs/*" \
    --conf spark.executor.extraClassPath="./build/user-libs/*" \
    --class Main ./build/libs/spark-experiments.jar 100

```

Check what was inserted

```
mysql --host=127.0.0.1 --port=3306 \
 --database=my_db --user=my --password=my
```

## Streaming

Configure Kafka

```
docker-compose up --build topics
```

Kafka to file streaming job:

```
distros_dir=./distros/spark-3.4.0-bin-hadoop3

./gradlew clean build copyAllDependencies

${distros_dir}/bin/spark-submit \
    --conf spark.driver.extraClassPath="./build/user-libs/*" \
    --conf spark.executor.extraClassPath="./build/user-libs/*" \
    --class MainStreaming ./build/libs/spark-experiments.jar ./chk ./output

```

Write to Kafka

```
kcat -b localhost:19092 -t raw -K: -P -l data-samples/sample1.json
```

## Minikube

Requires:
- kubectl 1.22.2 or higher
- minikube 1.29.0 or higher

Add custom stuff into image:

```shell
distros_dir=./distros/spark-3.4.0-bin-hadoop3

mkdir -p ${distros_dir}/user-jars

./gradlew copyAllDependencies -PcopyDependenciesInto=${distros_dir}/user-jars

cp Dockerfile ${distros_dir}/kubernetes/dockerfiles/spark/

${distros_dir}/bin/docker-image-tool.sh \
    -r my-spark.foo/my-spark -t 3.4.0-java11 build

minikube image load my-spark.foo/my-spark/spark:3.4.0-java11 --overwrite=true

```

When image is loaded, follow the scripts under [k8s dir](./k8s).

Open shell:

```
kubectl -n my-spark exec -it spark-client -- sh -c 'cd /opt/spark/; ./bin/spark-shell \
    --master k8s://https://kubernetes.default.svc:443 \
    --deploy-mode client \
    --conf spark.kubernetes.namespace=my-spark \
    --conf spark.kubernetes.container.image=my-spark.foo/my-spark/spark:3.4.0-java11 \
    --conf spark.kubernetes.container.image.pullPolicy=Never  \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=my-spark-sa \
    --conf spark.kubernetes.authenticate.serviceAccountName=my-spark-sa \
    --conf spark.kubernetes.driver.pod.name=spark-client \
    --conf spark.kubernetes.executor.podTemplateFile=file:///etc/spark/executor-template.yaml \
    --conf spark.driver.memory=512M \
    --conf spark.driver.host=spark-client-headless \
    --conf spark.driver.port=19987 \
    --conf spark.driver.extraClassPath="/opt/spark/user-jars/*" \
    --conf spark.executor.instances=1 \
    --conf spark.executor.memory=1G \
    --conf spark.executor.extraClassPath="/opt/spark/user-jars/*" \
    --conf spark.jars.ivy=/tmp/.ivy'

```

```scala
import org.apache.spark.sql.SaveMode

val df = spark.read.parquet("/opt/spark/examples/src/main/resources/users.parquet")

df.show

val url = "jdbc:mysql://mysql:3306/my_db?rewriteBatchedStatements=true"
val user = "my"
val pass = "my"
val table = "my_table"

df.select("name").write
  .format("jdbc")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("url", url)
  .option("user", user)
  .option("password", pass)
  .option("dbtable", table)
  .option("truncate", "true")
  .option("numPartitions", "4")
  .option("batchsize", "10000")
  .mode(SaveMode.Overwrite)
  .save

spark.read.format("jdbc")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("url", url)
  .option("user", user)
  .option("password", pass)
  .option("dbtable", table)
  .load
  .show

sys.exit

```

Once finished, you can clean up via:

```
minikube delete --all --purge
```
