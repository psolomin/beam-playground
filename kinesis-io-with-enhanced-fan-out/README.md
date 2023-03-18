# Kinesis IO with Enhanced Fan-Out

This project is used for testing Kinesis connector of Apache Beam. Testing was done with:

- Direct runner
- Flink runner (with toy Flink cluster in Docker)
- Kinesis Data Analytics ("serverless" Flink)

Testing approach consisted of the following:

1. start consumer with file sink (parquet)
2. start producer with known output records
3. (optionally)
	- re-shard Kinesis stream
	- app kill and start from savepoint
	- app start at some timestamp
	- run with increased network latency (via `tc`)
	- run with artificial "slow" processor (see `processTimePerRecord` cmd argument)
	- run with a sometimes-failing processor (see `failAfterRecordsSeenCnt` cmd argument)
4. check file sink outputs (with `pyspark`)

## Requirements

```
$ java -version
openjdk version "11.0.11" 2021-04-20
OpenJDK Runtime Environment AdoptOpenJDK-11.0.11+9 (build 11.0.11+9)
OpenJDK 64-Bit Server VM AdoptOpenJDK-11.0.11+9 (build 11.0.11+9, mixed mode)

export JAVA_HOME=<your jdk location>
export AWS_ACCOUNT=<your account id>
export AWS_PROFILE=<your profile>
export AWS_REGION=<your region>
export S3_BUCKET=<your artifacts bucket>
export STREAM=stream-01
export ROLE=BeamKdaAppRole
export CONSUMER_ARN=<your EFO consumer ARN>

# alternative - create a .env file with vars and export it:
export $(cat .env | xargs)
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

## AWS resources

Create stream and consumer:

```
aws kinesis create-stream --stream-name $STREAM \
	--shard-count 2 \
	--stream-mode-details=StreamMode=PROVISIONED

aws kinesis register-stream-consumer \
	--stream-arn arn:aws:kinesis:${AWS_REGION}:${AWS_ACCOUNT}:stream/$STREAM \
	--consumer-name consumer-01
```


Delete consumer and stream:

```
aws kinesis deregister-stream-consumer \
	--stream-arn arn:aws:kinesis:${AWS_REGION}:${AWS_ACCOUNT}:stream/$STREAM \
	--consumer-name consumer-01

aws kinesis delete-stream $STREAM
```

## Run Producer

Beam

```
mvn package -DskipTests \
	-Dapp.main.class=com.psolomin.producer.Main

java -jar target/example-com.psolomin.producer.Main-bundled-0.1-SNAPSHOT.jar \
	--outputStream=$STREAM --msgsToWrite=30 \
	--awsRegion=$AWS_REGION \
	--msgsPerSec=1 --runner=DirectRunner

```

Plain

```
mvn package -DskipTests \
	-Dapp.main.class=com.psolomin.plainproducer.Main

java -jar target/example-com.psolomin.plainproducer.Main-bundled-0.1-SNAPSHOT.jar \
	$STREAM 10 3000 # 5 minutes
```

## Run Consumer

Beam (Direct runner):

```
mvn package -DskipTests \
	-Dapp.main.class=com.psolomin.consumer.Main

java -jar target/example-com.psolomin.consumer.Main-bundled-0.1-SNAPSHOT.jar \
	--inputStream=$STREAM \
	--sinkLocation=$(pwd)/output \
	--awsRegion=$AWS_REGION \
	--consumerArn=$CONSUMER_ARN \
	| tee log.txt

```

## Kinesis Data Analytics applications

These require Flink runner and new AWS resources.

Build producer & consumer apps

```
mvn package -Pkda -DskipTests \
	-Dapp.main.class=com.psolomin.kda.KdaProducer

mvn package -Pkda -DskipTests \
	-Dapp.main.class=com.psolomin.kda.KdaConsumer
```

Create KDA app

Check [scripts instructions](./scripts/README.md) for that

## Vanilla Flink

This was tested with Docker engine 20.10.22. Older Docker engines may yield errors.

Build artefact

```
mvn package -Pflink -DskipTests \
	-Dapp.main.class=com.psolomin.flink.FlinkConsumer
```

Start toy cluster

```
docker-compose up --build -d flink-tm
```

Simulating networking issues (optional)

```
docker exec --privileged kinesis-io-with-enhanced-fan-out-flink-tm-1 \
	tc qdisc add dev eth0 root netem delay 300ms
```

Submit Flink job

```
docker exec -u flink -it kinesis-io-with-enhanced-fan-out-flink-jm-1 flink run \
	--class com.psolomin.flink.FlinkConsumer --detached \
	/mnt/artifacts/example-com.psolomin.flink.FlinkConsumer-bundled-0.1-SNAPSHOT.jar \
	--kinesisSourceToConsumerMapping="{\"stream-01\": \"$CONSUMER_ARN\"}" \
	--awsRegion=eu-west-1 \
	--inputStream=stream-01 \
	--autoWatermarkInterval=10000 \
	--sinkLocation=/mnt/output \
	--externalizedCheckpointsEnabled=true \
	--checkpointingMode=EXACTLY_ONCE \
	--numConcurrentCheckpoints=1 \
	--checkpointTimeoutMillis=500000 \
	--checkpointingInterval=60000 \
	--minPauseBetweenCheckpoints=5000 \
	--stateBackend=rocksdb \
	--stateBackendStoragePath=file:///tmp/flink-state

```

Stop with a savepoint:

```
docker exec -u flink -it kinesis-io-with-enhanced-fan-out-flink-jm-1 bin/flink stop \
	--savepointPath file:///mnt/savepoints/pt0 \
	2b952811df3388df43891664c391fbdd
```

Start with a savepoint:

```
docker exec -u flink -it kinesis-io-with-enhanced-fan-out-flink-jm-1 flink run \
	-s file:///mnt/savepoints/pt0/savepoint-2b1333-d9028e19a3ef \
	...
	--kinesisSourceToConsumerMapping="{\"stream-01\": \"$CONSUMER_ARN\"}"

```

Stop cluster

```
docker-compose down -v
```
