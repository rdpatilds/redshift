# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3>=1.34"]
# ///
"""Carry Canvas Live Events into the POC over one Kinesis stream, read by Redshift
streaming ingestion and by a Firehose copy to S3.

Every resource is a row in RESOURCES holding its own exists/create/delete, so a rerun is a
no-op and cleanup.py tears the stack down by walking the same table backwards.
Run:  uv run streaming.py
"""
import json
import pathlib
import time
from typing import Callable, NamedTuple

import boto3
from botocore.exceptions import ClientError

from data_api import run

REGION = "us-east-1"
NAME = "canvas"
TAGS = {"Project": "canvas", "Environment": "poc"}
TAG_LIST = [{"Key": k, "Value": v} for k, v in TAGS.items()]
FIREHOSE_ROLE = "canvas-firehose"
REDSHIFT_ROLE = "canvas-redshift-kinesis"
PRODUCER_USER = "canvas-live-events"
PREFIX = "raw/live-events/"
HERE = pathlib.Path(__file__).parent
KEY_FILE = HERE / ".canvas-live-events.env"
GITIGNORE = HERE / ".gitignore"

session = boto3.Session(region_name=REGION)
ACCOUNT = session.client("sts").get_caller_identity()["Account"]
BUCKET = f"canvas-live-events-{ACCOUNT}"
STREAM_ARN = f"arn:aws:kinesis:{REGION}:{ACCOUNT}:stream/{NAME}"
BUCKET_ARN = f"arn:aws:s3:::{BUCKET}"
FIREHOSE_ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/{FIREHOSE_ROLE}"
REDSHIFT_ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/{REDSHIFT_ROLE}"

kinesis = session.client("kinesis")
s3 = session.client("s3")
iam = session.client("iam")
firehose = session.client("firehose")
rs = session.client("redshift-serverless")


class Resource(NamedTuple):
    name: str
    exists: Callable[[], str | None]
    create: Callable[[], None]
    delete: Callable[[], None] | None


def wait_for(status_of, target, label, attempts=60, seconds=5):
    status = None
    for _ in range(attempts):
        status = status_of()
        if status == target:
            print(f"{label} {target}")
            return
        time.sleep(seconds)
    raise SystemExit(f"{label} stuck at {status}")


def allow(actions, resources):
    return {"Effect": "Allow", "Action": actions, "Resource": resources}


def ensure_role(role_name, services, statements):
    trust = {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": {"Service": services}, "Action": "sts:AssumeRole"}]}
    try:
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust), Tags=TAG_LIST)
        print(f"created role {role_name}")
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"role {role_name} exists without its policy")
    iam.put_role_policy(RoleName=role_name, PolicyName=NAME,
                        PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": statements}))
    print(f"put inline policy on {role_name}")


def role_ready(role_name):
    try:
        iam.get_role_policy(RoleName=role_name, PolicyName=NAME)
    except iam.exceptions.NoSuchEntityException:
        return None
    return f"arn:aws:iam::{ACCOUNT}:role/{role_name}"


def delete_role(role_name):
    try:
        iam.delete_role_policy(RoleName=role_name, PolicyName=NAME)
    except iam.exceptions.NoSuchEntityException:
        pass
    try:
        iam.delete_role(RoleName=role_name)
        print(f"deleted role {role_name}")
    except iam.exceptions.NoSuchEntityException:
        print(f"role {role_name} already gone")


def stream_exists():
    try:
        summary = kinesis.describe_stream_summary(StreamName=NAME)["StreamDescriptionSummary"]
    except kinesis.exceptions.ResourceNotFoundException:
        return None
    return (f"{summary['StreamStatus']}, {summary['OpenShardCount']} shard, "
            f"{summary['RetentionPeriodHours']} h retention")


def stream_create():
    # 24 h retention is the Kinesis default, so there is nothing to set.
    kinesis.create_stream(StreamName=NAME, ShardCount=1, StreamModeDetails={"StreamMode": "PROVISIONED"}, Tags=TAGS)
    print(f"created stream {NAME}")
    wait_for(lambda: kinesis.describe_stream_summary(StreamName=NAME)["StreamDescriptionSummary"]["StreamStatus"],
             "ACTIVE", f"stream {NAME}")


def stream_delete():
    try:
        kinesis.delete_stream(StreamName=NAME, EnforceConsumerDeletion=True)
        print(f"deleted stream {NAME}")
    except kinesis.exceptions.ResourceNotFoundException:
        print(f"stream {NAME} already gone")


def bucket_exists():
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            return None
        raise
    return BUCKET


def bucket_create():
    s3.create_bucket(Bucket=BUCKET)
    s3.put_public_access_block(Bucket=BUCKET, PublicAccessBlockConfiguration={
        "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
    s3.put_bucket_tagging(Bucket=BUCKET, Tagging={"TagSet": TAG_LIST})
    print(f"created bucket {BUCKET}")


def bucket_delete():
    if not bucket_exists():
        print(f"bucket {BUCKET} already gone")
        return
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys})
            print(f"deleted {len(keys)} objects from {BUCKET}")
    s3.delete_bucket(Bucket=BUCKET)
    print(f"deleted bucket {BUCKET}")


def firehose_role_create():
    ensure_role(FIREHOSE_ROLE, ["firehose.amazonaws.com"], [
        allow(["kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:GetShardIterator",
               "kinesis:GetRecords", "kinesis:ListShards"], STREAM_ARN),
        allow(["s3:AbortMultipartUpload", "s3:GetBucketLocation", "s3:GetObject", "s3:ListBucket",
               "s3:ListBucketMultipartUploads", "s3:PutObject"], [BUCKET_ARN, f"{BUCKET_ARN}/*"])])


def firehose_exists():
    try:
        described = firehose.describe_delivery_stream(DeliveryStreamName=NAME)
    except firehose.exceptions.ResourceNotFoundException:
        return None
    return described["DeliveryStreamDescription"]["DeliveryStreamStatus"]


def firehose_create():
    deadline = time.time() + 90
    while True:
        try:
            firehose.create_delivery_stream(
                DeliveryStreamName=NAME, DeliveryStreamType="KinesisStreamAsSource",
                KinesisStreamSourceConfiguration={"KinesisStreamARN": STREAM_ARN, "RoleARN": FIREHOSE_ROLE_ARN},
                ExtendedS3DestinationConfiguration={
                    "RoleARN": FIREHOSE_ROLE_ARN, "BucketARN": BUCKET_ARN, "Prefix": PREFIX,
                    "ErrorOutputPrefix": "errors/", "CompressionFormat": "GZIP",
                    "BufferingHints": {"IntervalInSeconds": 60, "SizeInMBs": 1}},
                Tags=TAG_LIST)
            break
        except ClientError as e:
            if e.response["Error"]["Code"] != "InvalidArgumentException" or time.time() > deadline:
                raise
            print("firehose cannot assume the role yet, waiting for IAM to propagate")
            time.sleep(10)
    print(f"created delivery stream {NAME}")
    wait_for(firehose_exists, "ACTIVE", f"delivery stream {NAME}")


def firehose_delete():
    try:
        firehose.delete_delivery_stream(DeliveryStreamName=NAME)
    except firehose.exceptions.ResourceNotFoundException:
        print(f"delivery stream {NAME} already gone")
        return
    # The bucket cannot be emptied while Firehose is still flushing into it.
    for _ in range(60):
        if firehose_exists() is None:
            print(f"deleted delivery stream {NAME}")
            return
        time.sleep(5)
    print(f"delivery stream {NAME} still deleting, carrying on")


def redshift_role_create():
    ensure_role(REDSHIFT_ROLE, ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"], [
        allow(["kinesis:DescribeStreamSummary", "kinesis:GetShardIterator", "kinesis:GetRecords",
               "kinesis:DescribeStream", "kinesis:ListShards"], STREAM_ARN),
        allow("kinesis:ListStreams", "*")])


def namespace_role_arns():
    # get_namespace returns either a bare ARN or an IamRole(applyStatus=..., iamRoleArn=...) wrapper.
    return [r[r.index("arn:"):].rstrip(")") for r in rs.get_namespace(namespaceName=NAME)["namespace"]["iamRoles"]]


def set_namespace_roles(arns, default):
    rs.update_namespace(namespaceName=NAME, iamRoles=arns, defaultIamRoleArn=default)
    wait_for(lambda: rs.get_namespace(namespaceName=NAME)["namespace"]["status"], "AVAILABLE", f"namespace {NAME}")


def attachment_exists():
    return REDSHIFT_ROLE_ARN if REDSHIFT_ROLE_ARN in namespace_role_arns() else None


def attachment_create():
    set_namespace_roles(sorted(set(namespace_role_arns()) | {REDSHIFT_ROLE_ARN}), REDSHIFT_ROLE_ARN)
    print(f"attached {REDSHIFT_ROLE} to namespace {NAME}")


def attachment_delete():
    arns = namespace_role_arns()
    if REDSHIFT_ROLE_ARN not in arns:
        print(f"{REDSHIFT_ROLE} not attached to namespace {NAME}")
        return
    set_namespace_roles([a for a in arns if a != REDSHIFT_ROLE_ARN], "")
    print(f"detached {REDSHIFT_ROLE} from namespace {NAME}")


def producer_exists():
    try:
        iam.get_user_policy(UserName=PRODUCER_USER, PolicyName=NAME)
    except iam.exceptions.NoSuchEntityException:
        return None
    return PRODUCER_USER


def producer_create():
    try:
        iam.create_user(UserName=PRODUCER_USER, Tags=TAG_LIST)
        print(f"created user {PRODUCER_USER}")
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"user {PRODUCER_USER} exists without its policy")
    iam.put_user_policy(UserName=PRODUCER_USER, PolicyName=NAME, PolicyDocument=json.dumps(
        {"Version": "2012-10-17", "Statement": [
            allow(["kinesis:PutRecord", "kinesis:PutRecords", "kinesis:DescribeStream"], STREAM_ARN)]}))
    print(f"put inline policy on {PRODUCER_USER}")


def live_key_ids():
    try:
        return [k["AccessKeyId"] for k in iam.list_access_keys(UserName=PRODUCER_USER)["AccessKeyMetadata"]]
    except iam.exceptions.NoSuchEntityException:
        return []


def drop_keys():
    for key_id in live_key_ids():
        iam.delete_access_key(UserName=PRODUCER_USER, AccessKeyId=key_id)
        print(f"deleted access key {key_id}")


def producer_delete():
    drop_keys()
    try:
        iam.delete_user_policy(UserName=PRODUCER_USER, PolicyName=NAME)
    except iam.exceptions.NoSuchEntityException:
        pass
    try:
        iam.delete_user(UserName=PRODUCER_USER)
        print(f"deleted user {PRODUCER_USER}")
    except iam.exceptions.NoSuchEntityException:
        print(f"user {PRODUCER_USER} already gone")


def held_key_id():
    if not KEY_FILE.exists():
        return None
    for line in KEY_FILE.read_text().splitlines():
        if line.startswith("AWS_ACCESS_KEY_ID="):
            return line.split("=", 1)[1].strip()
    return None


def key_exists():
    key_id = held_key_id()
    return f"{key_id} in {KEY_FILE.name}" if key_id and key_id in live_key_ids() else None


def key_create():
    ignored = GITIGNORE.read_text().splitlines() if GITIGNORE.exists() else []
    if KEY_FILE.name not in ignored:
        GITIGNORE.write_text("\n".join(ignored + [KEY_FILE.name]) + "\n")
        print(f"added {KEY_FILE.name} to .gitignore")
    drop_keys()
    key = iam.create_access_key(UserName=PRODUCER_USER)["AccessKey"]
    KEY_FILE.write_text(f"AWS_ACCESS_KEY_ID={key['AccessKeyId']}\nAWS_SECRET_ACCESS_KEY={key['SecretAccessKey']}\n")
    print(f"wrote access key {key['AccessKeyId']} to {KEY_FILE.name}")


def key_delete():
    drop_keys()
    KEY_FILE.unlink(missing_ok=True)


def mv_exists():
    rows = run("select 1 from svv_mv_info where schema_name = 'raw' and name = 'live_events_mv'")
    return "raw.live_events_mv" if rows else None


def mv_create():
    sql = (HERE / "streaming.sql").read_text().replace("<role arn>", REDSHIFT_ROLE_ARN)
    for statement in [s.strip() for s in sql.split(";") if s.strip()]:
        run(statement)
        print(statement.splitlines()[0][:70])
    print("applied streaming.sql")


RESOURCES = [
    Resource(f"kinesis stream {NAME}", stream_exists, stream_create, stream_delete),
    Resource(f"s3 bucket {BUCKET}", bucket_exists, bucket_create, bucket_delete),
    Resource(f"iam role {FIREHOSE_ROLE}", lambda: role_ready(FIREHOSE_ROLE), firehose_role_create,
             lambda: delete_role(FIREHOSE_ROLE)),
    Resource(f"firehose delivery stream {NAME}", firehose_exists, firehose_create, firehose_delete),
    Resource(f"iam role {REDSHIFT_ROLE}", lambda: role_ready(REDSHIFT_ROLE), redshift_role_create,
             lambda: delete_role(REDSHIFT_ROLE)),
    Resource(f"namespace {NAME} iam role", attachment_exists, attachment_create, attachment_delete),
    Resource(f"iam user {PRODUCER_USER}", producer_exists, producer_create, producer_delete),
    Resource(f"access key for {PRODUCER_USER}", key_exists, key_create, key_delete),
    Resource("redshift streaming objects", mv_exists, mv_create, None),
]


def main():
    for resource in RESOURCES:
        found = resource.exists()
        if found:
            print(f"{resource.name}: {found}")
        else:
            print(f"{resource.name}: creating")
            resource.create()
    print()
    print("stream arn:    ", STREAM_ARN)
    print("bucket:        ", f"s3://{BUCKET}/{PREFIX}")
    print("firehose:      ", NAME)
    print("firehose role: ", FIREHOSE_ROLE_ARN)
    print("redshift role: ", REDSHIFT_ROLE_ARN)
    print("producer user: ", PRODUCER_USER, f"(keys in {KEY_FILE.name})")
    print("canvas config: ", f"kinesis_stream_name: {NAME}, aws_region: {REGION}")


if __name__ == "__main__":
    main()
