# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3>=1.34"]
# ///
"""Pull whatever is on the Kinesis stream into Redshift and show both landing zones.

Run:  uv run streaming_refresh.py
"""
import boto3

from data_api import run
from query import print_table
from streaming import BUCKET, PREFIX, REGION


def refresh():
    run('REFRESH MATERIALIZED VIEW "raw".live_events_mv')


def event_count():
    return run('SELECT COUNT(*) AS rows FROM "raw".live_events')[0]["rows"]


def recent_events(limit=20):
    return run('SELECT event_name, event_time, user_id, approximate_arrival_timestamp AS arrival'
               f' FROM "raw".live_events ORDER BY approximate_arrival_timestamp DESC LIMIT {limit}')


def s3_objects():
    return boto3.client("s3", region_name=REGION).list_objects_v2(Bucket=BUCKET, Prefix=PREFIX).get("Contents", [])


def main():
    refresh()
    print("refreshed raw.live_events_mv")
    print(event_count(), "rows in raw.live_events")
    print_table(recent_events(20))
    objects = s3_objects()
    print(f"\n{len(objects)} objects under s3://{BUCKET}/{PREFIX}")
    for o in sorted(objects, key=lambda o: o["LastModified"], reverse=True)[:10]:
        print(f"  {o['LastModified']:%Y-%m-%d %H:%M:%S}  {o['Size']:>7}  {o['Key']}")


if __name__ == "__main__":
    main()
