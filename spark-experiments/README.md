# Spark Experiments

Run tests

```
./gradlew test
```

Start database

```
docker-compose up --build -d db
```

Run tests

```
./gradlew test
```

Run app

```
./gradlew run --args="100"
```

Check what was inserted

```
mysql --host=127.0.0.1 --port=3306 \
 --database=my_db --user=my --password=my
```
