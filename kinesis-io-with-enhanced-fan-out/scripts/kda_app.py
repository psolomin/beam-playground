import argparse
from typing import Optional

import boto3
import logging
import pathlib

from mypy_boto3_kinesisanalyticsv2 import KinesisAnalyticsV2Client
from mypy_boto3_sts import STSClient
from mypy_boto3_s3 import S3Client


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def get_account_id(client: STSClient):
    return client.get_caller_identity()["Account"]


def upload_jar(client: S3Client, local_path: str, bucket: str, s3_path: str):
    logger.info("Uploading jar %s to %s - %s", local_path, bucket, s3_path)
    client.upload_file(
        Filename=local_path,
        Bucket=bucket,
        Key=s3_path
    )


def producer_properties(region: str, stream: str):
    return {
        "PropertyGroupId": "ProducerProperties",
        "PropertyMap": {
            "AwsRegion": region,
            "OutputStream": stream,
            "MsgsToWrite": "1000",
            "MsgsPerSec": "50"
        }
    }


def consumer_properties(region: str, stream: str, consumer_arn: Optional[str]):
    return {
        "PropertyGroupId": "ConsumerProperties",
        "PropertyMap": {
            "AwsRegion": region,
            "InputStream": stream,
            "ConsumerArn": consumer_arn
        }
    }


def name_to_props(region: str, name: str, stream: str, consumer_arn: Optional[str] = None):
    d = {
        "Producer": producer_properties(region, stream),
        "Consumer": consumer_properties(region, stream, consumer_arn)
    }
    return d[name]


def create_or_update_app(
        client: KinesisAnalyticsV2Client,
        region: str,
        name: str,
        stream: str,
        consumer_arn: Optional[str],
        role_arn: str,
        s3_bucket: str,
        app_artifact_path: str,
):
    bucket_arn = f"arn:aws:s3:::{s3_bucket}"
    app_cli_args = {"PropertyGroups": [name_to_props(region, name, stream, consumer_arn)]}
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
        "EnvironmentProperties": app_cli_args,
        "ApplicationCodeConfiguration": {
            "CodeContent": {
                "S3ContentLocation": {
                    "BucketARN": bucket_arn,
                    "FileKey": app_artifact_path
                }
            },
            "CodeContentType": "ZIPFILE"
        },
        "ApplicationSnapshotConfiguration": {
            "SnapshotsEnabled": True
        }
    }
    apps_list = client.list_applications(Limit=20)["ApplicationSummaries"]
    apps_names_list = [a["ApplicationName"] for a in apps_list]
    log_stream_arn = f"arn:aws:logs:{region}:{aws_account_id}:log-group:" \
                     f"/aws/kinesis-analytics/{name}:log-stream:kinesis-analytics-log-stream"

    if name not in apps_names_list:
        logger.info("Creating app %s", name)
        client.create_application(
            ApplicationName=name,
            RuntimeEnvironment="FLINK-1_15",
            ServiceExecutionRole=role_arn,
            ApplicationConfiguration=configuration,
            ApplicationMode="STREAMING",
            CloudWatchLoggingOptions=[{
                "LogStreamARN": log_stream_arn
            }]
        )
    else:
        logger.info("Updating app %s", name)
        app_details = list(filter(lambda d: d["ApplicationName"] == name, apps_list))[0]
        client.update_application(
            ApplicationName=name,
            CurrentApplicationVersionId=app_details["ApplicationVersionId"],
            ApplicationConfigurationUpdate={
                "ApplicationCodeConfigurationUpdate": {
                    "CodeContentTypeUpdate": "ZIPFILE",
                    "CodeContentUpdate": {
                        "S3ContentLocationUpdate": {
                            "BucketARNUpdate": bucket_arn,
                            "FileKeyUpdate": app_artifact_path,
                        }
                    }
                },
                "EnvironmentPropertyUpdates": app_cli_args,
            }
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", required=True)
    parser.add_argument("--role", required=True)
    parser.add_argument("--app-name", required=True, choices=["Producer", "Consumer"])
    parser.add_argument("--stream-name", required=True)
    parser.add_argument("--consumer-arn", required=False, default=None)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--jar-name", required=True)
    parser.add_argument("--jar-local-path", required=True)
    parser.add_argument("--jar-s3-path", required=True)
    args = parser.parse_args()

    kda_client: KinesisAnalyticsV2Client = boto3.client("kinesisanalyticsv2", args.region)
    sts_client: STSClient = boto3.client("sts")
    s3_client: S3Client = boto3.client("s3")
    aws_account_id = get_account_id(sts_client)
    app_role_arn = f"arn:aws:iam::{aws_account_id}:role/{args.role}"
    jar_local_key = pathlib.Path(args.jar_local_path) / args.jar_name
    jar_s3_key = pathlib.Path(args.jar_s3_path) / args.jar_name

    upload_jar(
        s3_client,
        str(jar_local_key),
        args.bucket,
        str(jar_s3_key)
    )

    create_or_update_app(
        kda_client,
        args.region,
        args.app_name,
        args.stream_name,
        args.consumer_arn,
        app_role_arn,
        args.bucket,
        str(jar_s3_key)
    )
