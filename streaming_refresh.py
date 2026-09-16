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


def main():
    run('REFRESH MATERIALIZED VIEW "raw".live_events_mv')
    print("refreshed raw.live_events_mv")
    print(run('SELECT COUNT(*) AS rows FROM "raw".live_events')[0]["rows"], "rows in raw.live_events")
    print_table(run('SELECT event_name, event_time, user_id, approximate_arrival_timestamp AS arrival'
                    ' FROM "raw".live_events ORDER BY approximate_arrival_timestamp DESC LIMIT 20'))
    objects = boto3.client("s3", region_name=REGION).list_objects_v2(Bucket=BUCKET, Prefix=PREFIX).get("Contents", [])
    print(f"\n{len(objects)} objects under s3://{BUCKET}/{PREFIX}")
    for o in sorted(objects, key=lambda o: o["LastModified"], reverse=True)[:10]:
        print(f"  {o['LastModified']:%Y-%m-%d %H:%M:%S}  {o['Size']:>7}  {o['Key']}")


if __name__ == "__main__":
    main()
