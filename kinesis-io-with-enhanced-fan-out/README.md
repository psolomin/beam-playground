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
  --outputStream=stream-01 --msgsToWrite=300 \
  --awsRegion=eu-west-1 \
  --msgsPerSec=1 --runner=DirectRunner
```
