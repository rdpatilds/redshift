# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3>=1.34"]
# ///
"""Load the seed CSVs, mapping their placeholder user ids onto the live Canvas user ids."""
import argparse
import csv
import json
import os
import pathlib
import sys
import urllib.parse
import urllib.request

from data_api import run, run_many

CANVAS_URL = os.environ.get("CANVAS_URL", "http://localhost:3100")
CANVAS_TOKEN = os.environ.get("CANVAS_TOKEN", "cplatform-dev-token")
TIMESTAMP_COLUMNS = ("due_at", "submitted_at")
SPECS = [
    {"file": "seed/student_course_status.csv", "table": "nudges.student_course_status",
     "id_columns": ("user_id",), "quoted": ("name", "risk_level")},
    {"file": "seed/assignment_status.csv", "table": "nudges.assignment_status",
     "id_columns": ("user_id",), "quoted": ("title", "due_at", "submitted_at", "status")},
]


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def canvas_ids(rows):
    id_map = {}
    unresolved = []
    for row in rows:
        url = (f"{CANVAS_URL}/api/v1/accounts/1/users"
               f"?search_term={urllib.parse.quote(row['name'])}&per_page=50")
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {CANVAS_TOKEN}"})
        with urllib.request.urlopen(request) as response:
            found = json.load(response)
        match = next((user for user in found if user["name"] == row["name"]), None)
        if match is None:
            unresolved.append(row["name"])
        else:
            id_map[row["user_id"]] = match["id"]
    if unresolved:
        sys.exit("no Canvas user matched: " + ", ".join(unresolved))
    return id_map


def literal(column, value, spec, id_map):
    if value == "":
        return "NULL"
    if column in spec["id_columns"]:
        if value not in id_map:
            sys.exit(f"{spec['file']} has user_id {value}, which is not in the id map")
        return str(id_map[value])
    if column not in spec["quoted"]:
        return value
    if column in TIMESTAMP_COLUMNS:
        value = value.replace("T", " ") + ":00"
    return "'" + value.replace("'", "''") + "'"


def insert_sql(spec, rows, id_map):
    columns = list(rows[0])
    tuples = ["(" + ", ".join(literal(c, row[c], spec, id_map) for c in columns) + ")" for row in rows]
    return f"INSERT INTO {spec['table']} ({', '.join(columns)}) VALUES " + ", ".join(tuples)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--doc-ids", action="store_true")
    args = parser.parse_args()
    here = pathlib.Path(__file__).parent
    try:
        loads = [(spec, read_csv(here / spec["file"])) for spec in SPECS]
        students = loads[0][1]
        id_map = {row["user_id"]: int(row["user_id"]) for row in students} if args.doc_ids else canvas_ids(students)
        run_many([f"TRUNCATE {spec['table']}" for spec, _ in loads]
                 + [insert_sql(spec, rows, id_map) for spec, rows in loads])
        for row in students:
            print(f"{row['user_id']} -> {id_map[row['user_id']]} {row['name']}")
        for spec, _ in loads:
            count = run(f"SELECT COUNT(*) FROM {spec['table']}")[0]
            print(spec["table"], next(iter(count.values())))
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
