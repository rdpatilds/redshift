# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3>=1.34"]
# ///
"""Load one course's question pool from the seed CSV into nudges.question_pool."""
import argparse
import csv
import pathlib
import sys

from data_api import run, run_many

TABLE = "nudges.question_pool"
COLUMNS = ("course_id", "question_id", "topic", "difficulty", "question_type",
           "question_text", "answers", "points", "source")
QUOTED = ("topic", "difficulty", "question_type", "question_text", "source")
JSON_COLUMNS = ("answers",)


def read_rows(path, course_id):
    with open(path, newline="", encoding="utf-8") as handle:
        return [row for row in csv.DictReader(handle) if int(row["course_id"]) == course_id]


def literal(column, value):
    if value is None:
        return "NULL"
    if column not in QUOTED and column not in JSON_COLUMNS:
        return str(value)
    escaped = str(value).replace("'", "''")
    if column in JSON_COLUMNS:
        return "JSON_PARSE('" + escaped + "')"
    return "'" + escaped + "'"


def insert_sql(rows):
    tuples = ["(" + ", ".join(literal(c, row[c]) for c in COLUMNS) + ")" for row in rows]
    return f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES " + ", ".join(tuples)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--course", type=int, default=1)
    args = parser.parse_args()
    here = pathlib.Path(__file__).parent
    try:
        rows = read_rows(here / "seed/question_pool.csv", args.course)
        if not rows:
            sys.exit(f"course {args.course} has no question pool rows")
        run_many([f"DELETE FROM {TABLE} WHERE course_id = {args.course}", insert_sql(rows)])
        count = run(f"SELECT COUNT(*) FROM {TABLE} WHERE course_id = {args.course}")[0]
        print(TABLE, next(iter(count.values())))
        topics = run(f"SELECT topic, COUNT(*) AS count FROM {TABLE} WHERE course_id = {args.course}"
                     " GROUP BY topic ORDER BY topic")
        for row in topics:
            print(f"  {row['topic']}  {row['count']}")
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
