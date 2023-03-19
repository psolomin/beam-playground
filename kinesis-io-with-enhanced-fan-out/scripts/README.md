# Scripts for manipulating AWS resources

Install:

```
pip install -r requirements.txt

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

(Re-) Create IAM role and policy

```
python create_iam_resources.py \
  --region $AWS_REGION --bucket $S3_BUCKET --stream $STREAM --role $ROLE
```

Create KDA producer app

```
python kda_app.py \
  --region $AWS_REGION --role $ROLE --app-name Producer --bucket $S3_BUCKET \
  --stream $STREAM \
  --jar-name example-com.psolomin.kda.KdaProducer-bundled-0.1-SNAPSHOT.jar \
  --jar-local-path ../target \
  --jar-s3-path artifacts
```


Create KDA consumer app

```
python kda_app.py \
  --region $AWS_REGION --role $ROLE --app-name Consumer --bucket $S3_BUCKET \
  --stream $STREAM \
  --consumer-arn $CONSUMER_ARN \
  --jar-name example-com.psolomin.kda.KdaConsumer-bundled-0.1-SNAPSHOT.jar \
  --jar-local-path ../target \
  --jar-s3-path artifacts
```

Clean previous output

```
aws s3 rm s3://$S3_BUCKET/output/*
```

Check output:

```
output_dir =
spark.read.parquet(output_dir).count()
spark.read.parquet(output_dir).distinct().count()
spark.read.parquet(output_dir).groupBy("shard_id").count().show()
```
