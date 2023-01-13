# Scripts for manipulating AWS resources

Install:

```
pip install -r requirements.txt
export AWS_PROFILE=<your profile>
export AWS_REGION=<your region>
export S3_BUCKET=
export STREAM=
export ROLE=
```

(Re-) Create IAM role and policy

```
python create_iam_resources.py \
  --region $AWS_REGION --bucket $BUCKET --stream $STREAM --role $ROLE
```

Create KDA producer app

```
python kda_app.py \
  --region $AWS_REGION --app-name Producer --bucket $BUCKET \
  --jar-name example-com.psolomin.kda.KdaProducer-bundled-0.1-SNAPSHOT.jar \
  --jar-local-path ../target \
  --jar-s3-path artifacts
```
