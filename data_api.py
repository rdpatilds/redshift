# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3>=1.34"]
# ///
"""Redshift Data API calls under the caller's IAM identity, so no secret or DB user is sent."""
import time

import boto3

REGION = "us-east-1"
WORKGROUP = "canvas"
DATABASE = "dev"
VALUE_KEYS = ("longValue", "doubleValue", "stringValue", "booleanValue")

client = boto3.client("redshift-data", region_name=REGION)


def wait_finished(statement_id):
    while True:
        described = client.describe_statement(Id=statement_id)
        status = described["Status"]
        if status == "FINISHED":
            return described
        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(f"{status}: {described.get('Error', '')}")
        time.sleep(1)


def run(sql: str, parameters: list[dict] | None = None) -> list[dict]:
    kwargs = {"WorkgroupName": WORKGROUP, "Database": DATABASE, "Sql": sql}
    if parameters:
        kwargs["Parameters"] = parameters
    statement_id = client.execute_statement(**kwargs)["Id"]
    if not wait_finished(statement_id).get("HasResultSet"):
        return []
    rows = []
    columns = None
    token = None
    while True:
        page = client.get_statement_result(Id=statement_id, **({"NextToken": token} if token else {}))
        if columns is None:
            columns = [c["name"] for c in page["ColumnMetadata"]]
        for record in page["Records"]:
            values = [next((field[key] for key in VALUE_KEYS if key in field), None) for field in record]
            rows.append(dict(zip(columns, values)))
        token = page.get("NextToken")
        if not token:
            return rows


def run_many(sqls: list[str]) -> None:
    wait_finished(client.batch_execute_statement(WorkgroupName=WORKGROUP, Database=DATABASE, Sqls=sqls)["Id"])
