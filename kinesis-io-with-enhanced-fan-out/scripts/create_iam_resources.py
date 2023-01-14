import argparse
import boto3
import json
import logging

from mypy_boto3_iam import IAMClient
from mypy_boto3_sts import STSClient


logging.basicConfig(level=logging.INFO)
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
                    "s3:Get*",
                    "s3:List*"
                ],
                "Resource": [
                    f"arn:aws:s3:::{s3_bucket_name}",
                    f"arn:aws:s3:::{s3_bucket_name}/*"
                ]
            },
            {
                "Sid": "UseStream",
                "Effect": "Allow",
                "Action": "kinesis:*",
                "Resource": [
                    f"arn:aws:kinesis:{region}:{aws_account_id}:stream/{kinesis_stream_name}",
                    f"arn:aws:kinesis:{region}:{aws_account_id}:stream/{kinesis_stream_name}/*"
                ]
            },
            # logging
            {
                "Effect": "Allow",
                "Action": [
                    "autoscaling:Describe*",
                    "cloudwatch:*",
                    "logs:*",
                    "sns:*",
                    "iam:GetPolicy",
                    "iam:GetPolicyVersion",
                    "iam:GetRole",
                    "oam:ListSinks"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": "iam:CreateServiceLinkedRole",
                "Resource": "arn:aws:iam::*:role/aws-service-role/events.amazonaws.com/AWSServiceRoleForCloudWatchEvents*",
                "Condition": {
                    "StringLike": {
                        "iam:AWSServiceName": "events.amazonaws.com"
                    }
                }
            },
            {
                "Effect": "Allow",
                "Action": [
                    "oam:ListAttachedLinks"
                ],
                "Resource": "arn:aws:oam:*:*:sink/*"
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
        versions = client.list_policy_versions(PolicyArn=policy_arn)["Versions"]
        for version in versions:
            if not version["IsDefaultVersion"]:
                client.delete_policy_version(
                    PolicyArn=policy_arn, VersionId=version["VersionId"])

        client.delete_policy(PolicyArn=policy_arn)
    except client.exceptions.NoSuchEntityException:
        logger.warning("Attempted to delete non-existing %s", policy_arn)

    try:
        client.delete_role(RoleName=role_name)
    except client.exceptions.NoSuchEntityException:
        logger.warning("Attempted to delete non-existing %s", role_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--stream", required=True)
    parser.add_argument("--role", required=True)
    args = parser.parse_args()

    iam_client: IAMClient = boto3.client("iam")
    sts_client: STSClient = boto3.client("sts")
    aws_account_id = get_account_id(sts_client)
    destroy_if_exist_role_and_its_policies(
        iam_client,
        aws_account_id,
        args.role
    )
    create_role_and_its_policies(
        iam_client,
        args.region,
        args.role,
        args.bucket,
        args.stream
    )
