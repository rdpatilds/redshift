# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3>=1.34"]
# ///
"""Create the POC Redshift Serverless namespace and workgroup named `canvas`.

Idempotent: every resource is looked up by name before it is created, so a
rerun on an existing stack prints the same summary and changes nothing.
Run:  uv run provision.py
"""
import sys
import time

import boto3

REGION = "us-east-1"
NAME = "canvas"
TAGS = [{"key": "Project", "value": "canvas"}, {"key": "Environment", "value": "poc"}]
EC2_TAGS = [{"Key": t["key"], "Value": t["value"]} for t in TAGS] + [{"Key": "Name", "Value": "canvas-redshift"}]
REQUIRED_AZS = ["us-east-1a", "us-east-1b", "us-east-1c"]
CAPACITY_CANDIDATES = [4, 8]

ec2 = boto3.client("ec2", region_name=REGION)
rs = boto3.client("redshift-serverless", region_name=REGION)


def default_vpc_id():
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])["Vpcs"]
    if not vpcs:
        sys.exit("no default VPC in " + REGION)
    return vpcs[0]["VpcId"]


def ensure_subnets(vpc_id):
    subnets = {s["AvailabilityZone"]: s["SubnetId"] for s in ec2.describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}, {"Name": "default-for-az", "Values": ["true"]}])["Subnets"]}
    for az in REQUIRED_AZS:
        if az not in subnets:
            created = ec2.create_default_subnet(AvailabilityZone=az)["Subnet"]
            ec2.create_tags(Resources=[created["SubnetId"]], Tags=EC2_TAGS)
            subnets[az] = created["SubnetId"]
            print(f"created default subnet {created['SubnetId']} in {az}")
        else:
            print(f"subnet {subnets[az]} in {az} exists")
    return [subnets[az] for az in REQUIRED_AZS]


def ensure_namespace():
    try:
        ns = rs.get_namespace(namespaceName=NAME)["namespace"]
        print(f"namespace {NAME} exists ({ns['status']})")
    except rs.exceptions.ResourceNotFoundException:
        ns = rs.create_namespace(namespaceName=NAME, adminUsername="canvasadmin", manageAdminPassword=True,
                                 dbName="dev", tags=TAGS)["namespace"]
        print(f"created namespace {NAME}")
    return ns


def ensure_workgroup(subnet_ids):
    try:
        wg = rs.get_workgroup(workgroupName=NAME)["workgroup"]
        print(f"workgroup {NAME} exists ({wg['status']}, {wg['baseCapacity']} RPU)")
        return wg
    except rs.exceptions.ResourceNotFoundException:
        pass
    last_error = None
    for capacity in CAPACITY_CANDIDATES:
        try:
            wg = rs.create_workgroup(workgroupName=NAME, namespaceName=NAME, baseCapacity=capacity,
                                     maxCapacity=max(capacity, 8), publiclyAccessible=False,
                                     subnetIds=subnet_ids, tags=TAGS)["workgroup"]
            print(f"created workgroup {NAME} at {capacity} RPU")
            return wg
        except rs.exceptions.ValidationException as e:
            last_error = e
            print(f"{capacity} RPU rejected: {e}")
    sys.exit(f"no accepted capacity: {last_error}")


def wait_available(getter, key, label):
    for _ in range(120):
        status = getter()[key]["status"]
        if status == "AVAILABLE":
            print(f"{label} AVAILABLE")
            return
        print(f"{label} {status}, waiting")
        time.sleep(15)
    sys.exit(f"{label} never became AVAILABLE")


def main():
    subnet_ids = ensure_subnets(default_vpc_id())
    ensure_namespace()
    ensure_workgroup(subnet_ids)
    wait_available(lambda: rs.get_namespace(namespaceName=NAME), "namespace", "namespace")
    wait_available(lambda: rs.get_workgroup(workgroupName=NAME), "workgroup", "workgroup")
    ns = rs.get_namespace(namespaceName=NAME)["namespace"]
    wg = rs.get_workgroup(workgroupName=NAME)["workgroup"]
    print("namespace arn:", ns["namespaceArn"])
    print("workgroup arn:", wg["workgroupArn"])
    print("base capacity:", wg["baseCapacity"], "RPU, max", wg.get("maxCapacity"))
    print("subnets:", ",".join(subnet_ids))
    print("endpoint:", wg.get("endpoint", {}).get("address"))


if __name__ == "__main__":
    main()
