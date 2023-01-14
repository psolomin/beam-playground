# Kinesis IO with Enhanced Fan-Out

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
	--stream-arn arn:aws:kinesis:{AWS_REGION}:${ACCOUNT_ID}:stream/$STREAM \
	--consumer-name consumer-01
```


Delete consumer and stream:

```
aws kinesis deregister-stream-consumer \
	--stream-arn arn:aws:kinesis:{AWS_REGION}:${ACCOUNT_ID}:stream/$STREAM \
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
	$STREAM 10 5000
```

## Run Consumer

Beam (Direct runner):

```
mvn package -DskipTests \
	-Dapp.main.class=com.psolomin.consumer.Main

java -jar target/example-com.psolomin.consumer.Main-bundled-0.1-SNAPSHOT.jar \
	--inputStream=$STREAM \
	--consumerArn=arn:aws:kinesis:"$AWS_REGION":"$AWS_ACCOUNT":stream/"$STREAM"/consumer/consumer-01:1665959636 \
	--awsRegion=$AWS_REGION

```

Plain:

```

mvn package -DskipTests \
	-Dapp.main.class=com.psolomin.plainconsumer.Main

java -jar target/example-com.psolomin.plainconsumer.Main-bundled-0.1-SNAPSHOT.jar \
	stream-01 arn:aws:kinesis:"$AWS_REGION":"$AWS_ACCOUNT":stream/"$STREAM"/consumer/consumer-01:1665959636

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
