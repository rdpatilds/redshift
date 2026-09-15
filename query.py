# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3>=1.34"]
# ///
"""Run one statement or a whole .sql file against the POC Redshift workgroup."""
import argparse
import pathlib
import sys

from data_api import run, run_many


def resolve(path):
    given = pathlib.Path(path)
    return given if given.exists() else pathlib.Path(__file__).parent / path


def print_table(rows):
    if not rows:
        print("0 rows")
        return
    columns = list(rows[0])
    lines = [columns] + [["" if row[c] is None else str(row[c]) for c in columns] for row in rows]
    widths = [max(len(line[i]) for line in lines) for i in range(len(columns))]
    for line in lines:
        print("  ".join(cell.ljust(width) for cell, width in zip(line, widths)).rstrip())
    print(f"{len(rows)} rows")


def main():
    parser = argparse.ArgumentParser()
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--sql")
    target.add_argument("--file")
    args = parser.parse_args()
    try:
        if args.file:
            statements = [s for s in resolve(args.file).read_text().split(";") if s.strip()]
            run_many(statements)
            print(f"{len(statements)} statements applied")
        else:
            print_table(run(args.sql))
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
