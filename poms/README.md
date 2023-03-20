# POMs

Project used solely for creating a bundle of jars given some dependencies set.

1. Edit `pom.xml`

2. Run

```
mvn clean dependency:copy-dependencies
```

After that you can use `target/dependency` dir as a classpath. Example:

```
serialver -classpath "target/dependency/*" \
    org.apache.beam.sdk.io.aws2.kinesis.ShardCheckpoint

# prints
# org.apache.beam.sdk.io.aws2.kinesis.ShardCheckpoint:    private static final long serialVersionUID = 103536540299998471L;

serialver -classpath "target/dependency/*" \
    org.apache.beam.sdk.io.aws2.kinesis.StaticCheckpointGenerator

# prints
# org.apache.beam.sdk.io.aws2.kinesis.StaticCheckpointGenerator:    private static final long serialVersionUID = 5972850685627641931L;

```
