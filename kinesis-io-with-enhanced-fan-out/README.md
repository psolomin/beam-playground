# Kinesis IO with Enhanced Fan-Out

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

## AWS resources

Env:

```
ACCOUNT_ID=
export AWS_PROFILE=
export AWS_REGION=
```

Create stream and consumer:

```
aws kinesis create-stream --stream-name stream-01 \
	--shard-count 1 \
	--stream-mode-details=StreamMode=PROVISIONED

aws kinesis register-stream-consumer \
	--stream-arn arn:aws:kinesis:{AWS_REGION}:${ACCOUNT_ID}:stream/stream-01 \
	--consumer-name consumer-01
```

Update number of shards:

```
aws kinesis update-shard-count \
	--stream-name stream-01 --target-shard-count 2 --scaling-type UNIFORM_SCALING
```


Delete consumer and stream:

```
aws kinesis deregister-stream-consumer \
	--stream-arn arn:aws:kinesis:{AWS_REGION}:${ACCOUNT_ID}:stream/stream-01 \
	--consumer-name consumer-01

aws kinesis delete-stream stream-01
```

## Run Producer

Beam

```
mvn package -Ddirect-runner -DskipTests \
	-Dapp.main.class=com.psolomin.producer.Main

PRF=<your profile>
AWS_PROFILE=$PRF java -jar target/example-com.psolomin.producer.Main-bundled-0.1-SNAPSHOT.jar \
	--outputStream=stream-01 --msgsToWrite=30 \
	--awsRegion=eu-west-1 \
	--msgsPerSec=1 --runner=DirectRunner

```

Plain

```
mvn package -Ddirect-runner -DskipTests \
	-Dapp.main.class=com.psolomin.plainproducer.Main

AWS_REGION=eu-west-1 AWS_PROFILE=$PRF java -jar \
	target/example-com.psolomin.plainproducer.Main-bundled-0.1-SNAPSHOT.jar \
	stream-01 10 5000
```

## Run Consumer

Beam (WIP):

```
mvn package -Ddirect-runner -DskipTests \
	-Dapp.main.class=com.psolomin.consumer.Main

PRF=<your profile>
AWS_PROFILE=$PRF java -jar target/example-com.psolomin.consumer.Main-bundled-0.1-SNAPSHOT.jar \
	--inputStream=stream-01 \
	--awsRegion=eu-west-1

```

Plain:

```

mvn package -Ddirect-runner -DskipTests \
	-Dapp.main.class=com.psolomin.plainconsumer.Main

AWS_REGION=eu-west-1 AWS_PROFILE=$PRF java -jar \
	target/example-com.psolomin.plainconsumer.Main-bundled-0.1-SNAPSHOT.jar \
	stream-01 arn:aws:kinesis:eu-west-1:790288347884:stream/stream-01/consumer/consumer-01:1665959636

```
