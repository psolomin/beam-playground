# Kinesis IO with Enhanced Fan-Out

## Build

```
mvn install -DskipTests
```

## Test

```
mvn clean test
```

## Run Producer

```
mvn clean package -Ddirect-runner -DskipTests \
  -Dapp.main.class=com.psolomin.producer.Main

PRF=<your profile>
AWS_PROFILE=$PRF java -jar target/example-com.psolomin.producer.Main-bundled-0.1-SNAPSHOT.jar \
  --outputStream=stream-01 --msgsToWrite=30 \
  --awsRegion=eu-west-1 \
  --msgsPerSec=1 --runner=DirectRunner
```

## Run Consumer

```
mvn clean package -Ddirect-runner -DskipTests \
  -Dapp.main.class=com.psolomin.consumer.Main

PRF=<your profile>
AWS_PROFILE=$PRF java -jar target/example-com.psolomin.consumer.Main-bundled-0.1-SNAPSHOT.jar \
  --inputStream=stream-01 \
  --awsRegion=eu-west-1


mvn clean package -Ddirect-runner -DskipTests \
  -Dapp.main.class=com.psolomin.consumer.EnhancedFanOutConsumer

AWS_REGION=eu-west-1 AWS_PROFILE=$PRF java -jar \
  target/example-com.psolomin.consumer.EnhancedFanOutConsumer-bundled-0.1-SNAPSHOT.jar

```
