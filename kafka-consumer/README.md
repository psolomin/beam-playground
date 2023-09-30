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


Flink runner

```
mvn clean package -Pflink -DskipTests -Dapp.main.class=com.kfk.consumer.FlinkRunnerMain

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

```
