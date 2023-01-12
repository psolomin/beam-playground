import logging

import boto3, json

from mypy_boto3_iam import IAMClient
from mypy_boto3_sts import STSClient


logger = logging.getLogger()


def get_account_id(client: STSClient):
    return client.get_caller_identity()["Account"]


def create_role_and_its_policies(
        client: IAMClient,
        region: str,
        role_name: str,
        s3_bucket_name: str,
        kinesis_stream_name: str
):
    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": {
            "Effect": "Allow",
            "Principal": {
                "Service": "kinesisanalytics.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    }

    client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(assume_role_policy)
    )

    kda_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ReadCodeAndWriteOutput",
                "Effect": "Allow",
                "Action": [
                    "logs:DescribeLogGroups",
                    "s3:Get*",
                    "s3:List*"
                ],
                "Resource": [
                    f"arn:aws:logs:{region}::*",
                    f"arn:aws:s3:::{s3_bucket_name}",
                    f"arn:aws:s3:::{s3_bucket_name}/*"
                ]
            },
            {
                "Sid": "DescribeLogStreams",
                "Effect": "Allow",
                "Action": "logs:DescribeLogStreams",
                "Resource": f"arn:aws:logs:{region}::*"
            },
            {
                "Sid": "PutLogEvents",
                "Effect": "Allow",
                "Action": "logs:PutLogEvents",
                "Resource": f"arn:aws:logs:{region}::*"
            },
            {
                "Sid": "ListCloudwatchLogGroups",
                "Effect": "Allow",
                "Action": [
                    "logs:DescribeLogGroups"
                ],
                "Resource": [
                    f"arn:aws:logs:{region}::*"
                ]
            },
            {
                "Sid": "ReadInputStream",
                "Effect": "Allow",
                "Action": "kinesis:*",
                "Resource": f"arn:aws:kinesis:{region}::stream/{kinesis_stream_name}"
            },
            {
                "Sid": "WriteOutputStream",
                "Effect": "Allow",
                "Action": "kinesis:*",
                "Resource": f"arn:aws:kinesis:{region}::stream/{kinesis_stream_name}"
            }
        ]
    }

    policy_arn = client.create_policy(
        PolicyName=f"{role_name}Policy",
        PolicyDocument=json.dumps(kda_policy)
    )["Policy"]["Arn"]

    client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)


def destroy_if_exist_role_and_its_policies(
        client: IAMClient,
        account_id: str,
        role_name: str
):
    policy_name = f"{role_name}Policy"
    policy_arn = f"arn:aws:iam::{account_id}:policy/{policy_name}"

    try:
        client.detach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
    except client.exceptions.NoSuchEntityException:
        logger.warning("Attempted to detach failed: %s - %s", role_name, policy_arn)

    try:
        client.delete_policy(PolicyArn=policy_arn)
    except client.exceptions.NoSuchEntityException:
        logger.warning("Attempted to delete non-existing %s", policy_arn)

    try:
        client.delete_role(RoleName=role_name)
    except client.exceptions.NoSuchEntityException:
        logger.warning("Attempted to delete non-existing %s", role_name)


if __name__ == "__main__":
    aws_region = "eu-west-1"
    bucket_name = "p-beam-experiments"
    stream_name = "stream-01"
    iam_role_name = "BeamKdaAppRole"
    iam_client: IAMClient = boto3.client("iam")
    sts_client: STSClient = boto3.client("sts")
    aws_account_id = get_account_id(sts_client)
    destroy_if_exist_role_and_its_policies(
        iam_client,
        aws_account_id,
        iam_role_name
    )
    create_role_and_its_policies(
        iam_client,
        aws_region,
        iam_role_name,
        bucket_name,
        stream_name
    )
