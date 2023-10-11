# From Kafka to Files Pipeline

## Requirements

```
$ java -version
openjdk version "11.0.11" 2021-04-20
OpenJDK Runtime Environment AdoptOpenJDK-11.0.11+9 (build 11.0.11+9)
OpenJDK 64-Bit Server VM AdoptOpenJDK-11.0.11+9 (build 11.0.11+9, mixed mode)
```

## Build

```
mvn install -DskipTests
```

## Test

```
mvn test
```

## Apply formatting

```
mvn spotless:check
mvn spotless:apply
```

## Kafka

Create cluster and a topic in it:

```
docker-compose up --build topics
```

Write sample data

```
kcat -b localhost:19092 -t raw -K: -P -l data/input-sample.txt
kcat -b localhost:19092 -t raw2 -K: -P -l data/input-sample.txt
```

## Run Consumer

Direct runner:

```
mvn clean package -DskipTests -Dapp.main.class=com.kfk.consumer.DirectRunnerMain

java -jar target/example-com.kfk.consumer.DirectRunnerMain-bundled-0.1-SNAPSHOT.jar \
	--bootstrapServers=localhost:19092 --inputTopics=raw,raw2 --outputDir=output \
	--streaming=true

```


## Flink parallelism experiment

*Note:* jobs' submissions spit some errors, but the jobs actually run anyway, as Flink UI shows in `localhost:8081`.

```
mvn clean
mvn package -Pflink -DskipTests -Dapp.main.class=com.kfk.consumer.FlinkRunnerMain
mvn package -Pflink -DskipTests -Dapp.main.class=com.kfk.producer.Main

docker-compose up --build topics
docker-compose up --build -d flink-tm1

docker exec -u flink -it kafka-consumer-flink-jm-1 flink run \
	--class com.kfk.consumer.FlinkRunnerMain --detached \
	/mnt/artifacts/example-com.kfk.consumer.FlinkRunnerMain-bundled-0.1-SNAPSHOT.jar \
	--bootstrapServers=kafka:9092 --inputTopics=raw,raw2 --outputDir=/mnt/output \
	--autoWatermarkInterval=10000 \
	--externalizedCheckpointsEnabled=true \
	--checkpointingMode=EXACTLY_ONCE \
	--numConcurrentCheckpoints=1 \
	--checkpointTimeoutMillis=500000 \
	--checkpointingInterval=60000 \
	--minPauseBetweenCheckpoints=5000 \
	--stateBackend=rocksdb \
	--stateBackendStoragePath=file:///tmp/flink-state \
	--parallelism=2

docker exec -u flink -it kafka-consumer-flink-jm-1 flink run \
	--class com.kfk.producer.Main --detached \
	/mnt/artifacts/example-com.kfk.producer.Main-bundled-0.1-SNAPSHOT.jar \
	--bootstrapServers=kafka:9092 --outputTopic=raw --msgsToWrite=20000 --msgsPerSec=100 \
	--parallelism=1

# step 1 - wait for some files to be written and stop with a snapshot

docker exec -u flink -it kafka-consumer-flink-jm-1 bin/flink stop \
	--savepointPath file:///mnt/savepoints \
	< some flink job id >

# step 2 - wait for some time and re-start the job from a savepoint

docker exec -u flink -it kafka-consumer-flink-jm-1 flink run \
	-s file:///mnt/savepoints/savepoint-< some savepoint id > \
	--class com.kfk.consumer.FlinkRunnerMain --detached \
	/mnt/artifacts/example-com.kfk.consumer.FlinkRunnerMain-bundled-0.1-SNAPSHOT.jar \
	--bootstrapServers=kafka:9092 --inputTopics=raw,raw2 --outputDir=/mnt/output \
	--autoWatermarkInterval=10000 \
	--externalizedCheckpointsEnabled=true \
	--checkpointingMode=EXACTLY_ONCE \
	--numConcurrentCheckpoints=1 \
	--checkpointTimeoutMillis=500000 \
	--checkpointingInterval=60000 \
	--minPauseBetweenCheckpoints=5000 \
	--stateBackend=rocksdb \
	--stateBackendStoragePath=file:///tmp/flink-state \
	--parallelism=2

```

This should eventually make the local [output dir](./output/raw) to contain 20K records in JSON format.
This experiment step is to make sure that savepoint behaves as expected.

Can be validated via Spark:

```
>>> spark.read.json("output/raw").count()
20000                                                                           
>>> spark.read.json("output/raw").select("id").distinct().count()
20000
```

Now, starting consumer with parallelism = 3, making a savepoint, and restarting from it with parallelism = 2:

```
# clean-up all state:

rm -rf output/raw
docker-compose down -v

docker-compose up --build topics
docker-compose up --build -d flink-tm1

docker exec -u flink -it kafka-consumer-flink-jm-1 flink run \
	--class com.kfk.consumer.FlinkRunnerMain --detached \
	/mnt/artifacts/example-com.kfk.consumer.FlinkRunnerMain-bundled-0.1-SNAPSHOT.jar \
	--bootstrapServers=kafka:9092 --inputTopics=raw,raw2 --outputDir=/mnt/output \
	--autoWatermarkInterval=10000 \
	--externalizedCheckpointsEnabled=true \
	--checkpointingMode=EXACTLY_ONCE \
	--numConcurrentCheckpoints=1 \
	--checkpointTimeoutMillis=500000 \
	--checkpointingInterval=60000 \
	--minPauseBetweenCheckpoints=5000 \
	--stateBackend=rocksdb \
	--stateBackendStoragePath=file:///tmp/flink-state \
	--parallelism=3

docker exec -u flink -it kafka-consumer-flink-jm-1 flink run \
	--class com.kfk.producer.Main --detached \
	/mnt/artifacts/example-com.kfk.producer.Main-bundled-0.1-SNAPSHOT.jar \
	--bootstrapServers=kafka:9092 --outputTopic=raw --msgsToWrite=20000 --msgsPerSec=100 \
	--parallelism=1

# step 1 - wait for some files to be written and stop with a snapshot

docker exec -u flink -it kafka-consumer-flink-jm-1 bin/flink stop \
	--savepointPath file:///mnt/savepoints \
	< some flink job id >

# step 2 - wait for some time and re-start the job from a savepoint

docker exec -u flink -it kafka-consumer-flink-jm-1 flink run \
	-s file:///mnt/savepoints/savepoint-< savepoint id > \
	--class com.kfk.consumer.FlinkRunnerMain --detached \
	/mnt/artifacts/example-com.kfk.consumer.FlinkRunnerMain-bundled-0.1-SNAPSHOT.jar \
	--bootstrapServers=kafka:9092 --inputTopics=raw,raw2 --outputDir=/mnt/output \
	--autoWatermarkInterval=10000 \
	--externalizedCheckpointsEnabled=true \
	--checkpointingMode=EXACTLY_ONCE \
	--numConcurrentCheckpoints=1 \
	--checkpointTimeoutMillis=500000 \
	--checkpointingInterval=60000 \
	--minPauseBetweenCheckpoints=5000 \
	--stateBackend=rocksdb \
	--stateBackendStoragePath=file:///tmp/flink-state \
	--parallelism=2

# clean-up all state:

docker-compose down -v

```
