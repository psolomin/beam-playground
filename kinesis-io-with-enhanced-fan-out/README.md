# Kinesis IO with Enhanced Fan-Out

## Build

```
mvn install -DskipTests
```

## Test

```
mvn clean test
```

## Run

```
mvn clean package -Ddirect-runner -DskipTests
java -jar target/example-bundled-0.1-SNAPSHOT.jar
```
