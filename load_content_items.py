# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3>=1.34"]
# ///
"""Load one course's live Canvas module items into nudges.content_items, tagged from the seed CSV."""
import argparse
import csv
import json
import os
import pathlib
import sys
import urllib.request

from data_api import run, run_many

CANVAS_URL = os.environ.get("CANVAS_URL", "http://localhost:3100")
CANVAS_TOKEN = os.environ.get("CANVAS_TOKEN", "cplatform-dev-token")
TABLE = "nudges.content_items"
COLUMNS = ("course_id", "module_id", "module_position", "module_name", "module_item_id",
           "item_position", "item_type", "content_id", "title", "url",
           "topics", "difficulty", "is_practice")
QUOTED = ("module_name", "item_type", "title", "url", "topics", "difficulty")
PREVIEW = ("module_position", "item_position", "item_type", "title", "topics", "difficulty", "is_practice")


def read_tags(path):
    with open(path, newline="", encoding="utf-8") as handle:
        return {row["title"]: row for row in csv.DictReader(handle)}


def canvas_modules(course_id):
    url = (f"{CANVAS_URL}/api/v1/courses/{course_id}/modules"
           "?include%5B%5D=items&per_page=50")
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {CANVAS_TOKEN}"})
    with urllib.request.urlopen(request) as response:
        return json.load(response)


def build_rows(course_id, modules, tags):
    rows = []
    for module in sorted(modules, key=lambda m: m["position"]):
        for item in sorted(module.get("items", []), key=lambda i: i["position"]):
            tag = tags.get(item["title"])
            if tag is None:
                print(f"no content_tags.csv row for {item['title']}, loading it untagged")
            rows.append({
                "course_id": course_id,
                "module_id": module["id"],
                "module_position": module["position"],
                "module_name": module["name"],
                "module_item_id": item["id"],
                "item_position": item["position"],
                "item_type": item["type"],
                "content_id": item.get("content_id"),
                "title": item["title"],
                "url": item["html_url"],
                "topics": tag["topics"] if tag else None,
                "difficulty": tag["difficulty"] if tag else None,
                "is_practice": "TRUE" if tag and tag["is_practice"] == "true" else "FALSE",
            })
    return rows


def literal(column, value):
    if value is None:
        return "NULL"
    if column not in QUOTED:
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def insert_sql(rows):
    tuples = ["(" + ", ".join(literal(c, row[c]) for c in COLUMNS) + ")" for row in rows]
    return f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES " + ", ".join(tuples)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--course", type=int, default=1)
    args = parser.parse_args()
    here = pathlib.Path(__file__).parent
    try:
        tags = read_tags(here / "seed/content_tags.csv")
        rows = build_rows(args.course, canvas_modules(args.course), tags)
        if not rows:
            sys.exit(f"course {args.course} has no module items")
        run_many([f"DELETE FROM {TABLE} WHERE course_id = {args.course}", insert_sql(rows)])
        count = run(f"SELECT COUNT(*) FROM {TABLE} WHERE course_id = {args.course}")[0]
        print(TABLE, next(iter(count.values())))
        preview = run(f"SELECT {', '.join(PREVIEW)} FROM {TABLE} WHERE course_id = {args.course}"
                      " ORDER BY module_position, item_position LIMIT 3")
        for row in preview:
            print("  " + "  ".join("" if row[c] is None else str(row[c]) for c in PREVIEW))
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
