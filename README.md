# Redshift POC

Redshift Serverless behind the Canvas nudges POC. Each script is a flat PEP 723 script run with `uv run`, so there is nothing to install first.

## Scripts

- `provision.py` creates the Redshift Serverless namespace and workgroup named `canvas`, plus the default subnets they need.
- `data_api.py` is the shared Redshift Data API helper. The other scripts import `run` and `run_many` from it.
- `schema.sql` holds the `nudges` schema and its six tables. Every statement is rerunnable. `learning_paths` is one ordered row per step of a student's path, written by the nudge agent's `path` command and regenerated on every scan.
- `query.py` runs one statement from `--sql` and prints the rows, or applies a whole file with `--file`.
- `load_seed.py` loads both CSVs under `seed/` into the two status tables, truncating first so a rerun is clean. It resolves each seed student name against the live Canvas account and inserts the real Canvas user id.
- `migrate.py` applies every file under `migrations/` in name order and skips each `ADD COLUMN` whose column is already there, because Redshift has no `ADD COLUMN IF NOT EXISTS`. A new table needs no migration. It goes into `schema.sql`, which is rerunnable, and `CREATE TABLE IF NOT EXISTS` makes the second apply a no-op.
- `load_content_items.py` reads the live Canvas module items for one course, joins them with `seed/content_tags.csv`, and loads `nudges.content_items`. An item inside a module named `Remediation: <topic>` with no CSV row is tagged with that topic and `is_practice` true, so quizzes the agent generates into a remediation module stay tagged across reloads.
- `load_question_pool.py` reads `seed/question_pool.csv`, keeps only the rows for one course, and loads `nudges.question_pool`. It deletes that course's rows and inserts the pool in one batch, then prints the total and a count per topic. `--course` defaults to 1.
- `cleanup.py` lists and deletes the tagged resources.

**`seed/question_pool.csv` is not Kaplan QBank.** The questions in it were written for this POC, because the POC needed a question source and had no QBank access. No Kaplan QBank content was used or copied. The `source` column records `qbank-stand-in` on every row, so the provenance is queryable.

## Run order

```
uv run provision.py
uv run query.py --file schema.sql
uv run load_seed.py
uv run migrate.py
uv run load_content_items.py
uv run load_question_pool.py
uv run cleanup.py
uv run cleanup.py --yes
```

The first `cleanup.py` is a dry run and only lists what it would delete. Add `--yes` to tear the stack down.

## Tags

Every resource carries `Project=canvas` and `Environment=poc`. `Project=canvas` is the key `cleanup.py` lists and deletes by. Anything created later for this POC has to carry it too, or cleanup will walk straight past it and leave it running.

## Access

All SQL goes through the Redshift Data API under the caller's own IAM identity. There is no database password to store or rotate, and no network path into the VPC to arrange. The workgroup is not publicly accessible.
