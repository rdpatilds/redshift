# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3>=1.34"]
# ///
"""Tear down everything tagged Project=canvas, plus the streaming stack that streaming.py
built, walked backwards through the same resource table. Dry run unless --yes is passed."""
import argparse
import time

import boto3
from botocore.exceptions import ClientError

from streaming import RESOURCES

REGION = "us-east-1"
NAME = "canvas"
TAG_FILTERS = [{"Key": "Project", "Values": ["canvas"]}]
POLL_ATTEMPTS = 60
POLL_SECONDS = 5

tagging = boto3.client("resourcegroupstaggingapi", region_name=REGION)
rs = boto3.client("redshift-serverless", region_name=REGION)
ec2 = boto3.client("ec2", region_name=REGION)


def tagged_arns():
    arns = []
    token = ""
    while True:
        page = tagging.get_resources(TagFilters=TAG_FILTERS, PaginationToken=token)
        arns.extend(r["ResourceARN"] for r in page["ResourceTagMappingList"])
        token = page.get("PaginationToken", "")
        if not token:
            return arns


def wait_gone(getter, label):
    for _ in range(POLL_ATTEMPTS):
        try:
            getter()
        except rs.exceptions.ResourceNotFoundException:
            print(f"{label} deleted")
            return
        time.sleep(POLL_SECONDS)
    print(f"{label} still present after {POLL_ATTEMPTS * POLL_SECONDS}s, giving up")


def delete_workgroup():
    try:
        rs.delete_workgroup(workgroupName=NAME)
    except rs.exceptions.ResourceNotFoundException:
        print(f"workgroup {NAME} already gone")
        return
    wait_gone(lambda: rs.get_workgroup(workgroupName=NAME), f"workgroup {NAME}")


def delete_namespace():
    try:
        rs.delete_namespace(namespaceName=NAME)
    except rs.exceptions.ResourceNotFoundException:
        print(f"namespace {NAME} already gone")
        return
    wait_gone(lambda: rs.get_namespace(namespaceName=NAME), f"namespace {NAME}")


def delete_subnets(arns):
    for arn in arns:
        if ":subnet/" not in arn:
            continue
        subnet_id = arn.rsplit("/", 1)[1]
        try:
            ec2.delete_subnet(SubnetId=subnet_id)
            print(f"deleted subnet {subnet_id}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "InvalidSubnetID.NotFound":
                raise
            print(f"subnet {subnet_id} already gone")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()
    arns = tagged_arns()
    for arn in arns:
        print(arn)
    for resource in RESOURCES:
        print(resource.name)
    if not args.yes:
        print("dry run, pass --yes to delete")
        return
    for resource in reversed(RESOURCES):
        if resource.delete:
            resource.delete()
    delete_workgroup()
    delete_namespace()
    delete_subnets(arns)
    remaining = tagged_arns()
    print(f"{len(remaining)} tagged resources remain")
    for arn in remaining:
        print(arn)


if __name__ == "__main__":
    main()
