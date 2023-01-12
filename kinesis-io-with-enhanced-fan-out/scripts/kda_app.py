import boto3

from mypy_boto3_kinesisanalyticsv2 import KinesisAnalyticsV2Client
from mypy_boto3_sts import STSClient


def get_account_id(client: STSClient):
    return client.get_caller_identity()["Account"]


def create_app(
        client: KinesisAnalyticsV2Client,
        name: str,
        role_arn: str,
        s3_bucket: str,
        app_artifact_path: str,
):
    configuration = {
        "FlinkApplicationConfiguration": {
            "CheckpointConfiguration": {
                "ConfigurationType": "CUSTOM",
                "CheckpointingEnabled": True,
                "CheckpointInterval": 60000,
                "MinPauseBetweenCheckpoints": 5000
            },
            "MonitoringConfiguration": {
                "ConfigurationType": "CUSTOM",
                "LogLevel": "INFO",
                "MetricsLevel": "APPLICATION"
            },
            "ParallelismConfiguration": {
                "ConfigurationType": "CUSTOM",
                "AutoScalingEnabled": False,
                "Parallelism": 2,
                "ParallelismPerKPU": 1
            },
        },
        "EnvironmentProperties": {
            "PropertyGroups": [
                {
                    "PropertyGroupId": "BeamApplicationProperties",
                    "PropertyMap": {
                        "InputStreamName": "steam-01",
                    }
                }
            ]
        },
        "ApplicationCodeConfiguration": {
            "CodeContent": {
                "S3ContentLocation": {
                    "BucketARN": f"arn:aws:s3:::{s3_bucket}",
                    "FileKey": app_artifact_path
                }
            },
            "CodeContentType": "ZIPFILE"
        },
        "ApplicationSnapshotConfiguration": {
            "SnapshotsEnabled": True
        }
    }
    client.create_application(
        ApplicationName=name,
        RuntimeEnvironment="FLINK-1_15",
        ServiceExecutionRole=role_arn,
        ApplicationConfiguration=configuration,
        ApplicationMode="STREAMING"
    )


if __name__ == "__main__":
    region = "eu-west-1"
    app_name = "Producer"
    aws_s3_bucket = "p-beam-experiments"
    artifact_s3_path = "artifacts/example-com.psolomin.kda.KdaProducer-bundled-0.1-SNAPSHOT.jar"
    kda_client: KinesisAnalyticsV2Client = boto3.client("kinesisanalyticsv2", region)
    sts_client: STSClient = boto3.client("sts")
    aws_account_id = get_account_id(sts_client)
    app_role_arn = f"arn:aws:iam::{aws_account_id}:role/BeamKdaAppRole"
    create_app(
        kda_client,
        app_name,
        app_role_arn,
        aws_s3_bucket,
        artifact_s3_path
    )
