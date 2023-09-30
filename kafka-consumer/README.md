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
```

## Run Consumer

Direct runner:

```
mvn clean package -DskipTests -Dapp.main.class=com.kfk.consumer.Main

java -jar target/example-com.kfk.consumer.Main-bundled-0.1-SNAPSHOT.jar \
  --bootstrapServers=localhost:19092 --inputTopics=raw,raw2 \
  --streaming=true

```
