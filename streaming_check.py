# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3>=1.34"]
# ///
"""Drive a small, deterministic set of Canvas changes that raise Live Events, then prove
they reached raw.live_events and S3 through the real pipeline.

Run:  uv run streaming_check.py
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from streaming_refresh import refresh, s3_objects
from data_api import run

CANVAS_URL = os.environ.get("CANVAS_URL", "http://localhost:3100")
CANVAS_TOKEN = os.environ.get("CANVAS_TOKEN", "cplatform-dev-token")
COURSE_ID = 1
STUDENT_ID = 6
FIREHOSE_WAIT_SECONDS = 90


def canvas_request(method, path, params):
    url = f"{CANVAS_URL}{path}"
    data = urllib.parse.urlencode(params).encode()
    request = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {CANVAS_TOKEN}",
        "Content-Type": "application/x-www-form-urlencoded",
    })
    try:
        with urllib.request.urlopen(request) as response:
            return json.load(response)
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {url} -> {e.code}: {e.read().decode()}")


def create_assignment(run_id):
    body = canvas_request("POST", f"/api/v1/courses/{COURSE_ID}/assignments", [
        ("assignment[name]", f"Live Events check {run_id}"),
        ("assignment[submission_types][]", "online_text_entry"),
        ("assignment[points_possible]", "10"),
        ("assignment[published]", "true"),
    ])
    print(f"assignment_created: id={body['id']}")
    return body["id"]


def submit_assignment(assignment_id, run_id):
    path = f"/api/v1/courses/{COURSE_ID}/assignments/{assignment_id}/submissions?as_user_id={STUDENT_ID}"
    body = canvas_request("POST", path, [
        ("submission[submission_type]", "online_text_entry"),
        ("submission[body]", f"Live Events check {run_id} submission"),
    ])
    print(f"submission_created: id={body['id']}")
    return body["id"]


def grade_submission(assignment_id):
    path = f"/api/v1/courses/{COURSE_ID}/assignments/{assignment_id}/submissions/{STUDENT_ID}"
    body = canvas_request("PUT", path, [("submission[posted_grade]", "8")])
    print(f"grade_change: submission id={body['id']} grade={body.get('grade')}")


def delete_assignment(assignment_id):
    canvas_request("DELETE", f"/api/v1/courses/{COURSE_ID}/assignments/{assignment_id}", [])
    print(f"deleted assignment {assignment_id}, Canvas raises assignment_updated for a soft delete")


def events_for_run(run_id):
    return run("SELECT event_name, event_time, user_id"
               ' FROM "raw".live_events'
               f" WHERE JSON_SERIALIZE(body) LIKE '%Live Events check {run_id}%'"
               " ORDER BY approximate_arrival_timestamp")


def main():
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_start = datetime.now(timezone.utc)
    print(f"run id {run_id}")

    assignment_id = create_assignment(run_id)
    submit_assignment(assignment_id, run_id)
    grade_submission(assignment_id)
    delete_assignment(assignment_id)

    print(f"waiting {FIREHOSE_WAIT_SECONDS}s for the Firehose buffer")
    time.sleep(FIREHOSE_WAIT_SECONDS)

    refresh()
    rows = events_for_run(run_id)
    seen = {row["event_name"] for row in rows}
    for row in rows:
        print(f"{row['event_name']:<20} {row['event_time']}  user_id={row['user_id']}")

    missing = {"assignment_created", "submission_created"} - seen
    if missing:
        sys.exit(f"raw.live_events is missing {sorted(missing)} for run {run_id} (assignment {assignment_id})")

    objects = s3_objects()
    newest = max((o["LastModified"] for o in objects), default=None)
    if newest is None or newest <= run_start:
        sys.exit(f"no S3 object under raw/live-events/ newer than run start {run_start.isoformat()}")
    print(f"newest S3 object: {newest:%Y-%m-%d %H:%M:%S}")

    print("streaming_check passed")


if __name__ == "__main__":
    main()
