# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3>=1.34"]
# ///
"""Apply every migrations/*.sql statement, skipping the ADD COLUMNs Redshift already has."""
import pathlib
import re
import sys

from data_api import run, run_many

ADD_COLUMN = re.compile(r"\s*ALTER\s+TABLE\s+(\w+)\.(\w+)\s+ADD\s+COLUMN\s+(\w+)", re.IGNORECASE)


def column_exists(schema, table, column):
    return bool(run("SELECT 1 FROM information_schema.columns"
                    f" WHERE table_schema = '{schema.lower()}'"
                    f" AND table_name = '{table.lower()}'"
                    f" AND column_name = '{column.lower()}'"))


def main():
    try:
        for path in sorted((pathlib.Path(__file__).parent / "migrations").glob("*.sql")):
            statements = [s for s in path.read_text().split(";") if s.strip()]
            for index, statement in enumerate(statements, 1):
                match = ADD_COLUMN.match(statement)
                if match is None:
                    run_many([statement])
                    print(f"{path.name} statement {index} applied")
                    continue
                schema, table, column = match.groups()
                target = f"{schema}.{table}.{column}"
                if column_exists(schema, table, column):
                    print(f"{path.name} {target} already present")
                else:
                    run_many([statement])
                    print(f"{path.name} {target} applied")
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
