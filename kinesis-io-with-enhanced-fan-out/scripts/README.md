# Scripts for manipulating AWS resources

Install:

```
pip install -r requirements.txt
export AWS_PROFILE=<your profile>
export AWS_REGION=<your region>
```

(Re-) Create IAM role and policy

```
python create_iam_resources.py
```

Create KDA app

```
python kda_app.py
```
