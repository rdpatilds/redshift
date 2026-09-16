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
- `streaming.py` creates the Kinesis stream that Canvas writes Live Events to, the Firehose copy to S3, the two IAM roles, the producer IAM user, and the Redshift objects that read the stream. See Streaming below.
- `streaming.sql` holds the external schema over Kinesis, the streaming materialized view, and the flattened view on top of it. `streaming.py` substitutes the IAM role ARN and applies it.
- `streaming_refresh.py` refreshes the materialized view and prints what landed in Redshift and in S3.
- `streaming_canvas.py` writes the Kinesis stream name, region, and producer credentials into the Canvas checkout's `config/dynamic_settings.yml` and restarts `web` and `jobs`. See Streaming below.
- `streaming_check.py` is the rerunnable end-to-end proof that Canvas Live Events reach Redshift and S3. See Streaming below.
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

## Streaming

Canvas Live Events reach the warehouse through one Kinesis stream with two readers. Redshift streaming ingestion polls the stream into a materialized view. A Firehose delivery stream writes the same records to S3 as gzipped JSON lines, which is the replay copy.

```
uv run streaming.py
uv run streaming_canvas.py
uv run streaming_check.py
```

`streaming.py` prints the values Canvas needs when it finishes. `streaming_canvas.py` writes them into the Canvas checkout's `config/dynamic_settings.yml` and restarts the `web` and `jobs` containers so the new config takes effect, then polls the API until it answers. It only wires the config and proves nothing by itself. `streaming_check.py` is the end-to-end proof. It creates an assignment, submits it as a student, and grades it, each tagged with a run id, then waits out the Firehose buffer and asserts the resulting `assignment_created` and `submission_created` events are in `raw.live_events` and that S3 got a fresh object. Rerun it any time to reprove the pipeline; each run's id keeps it distinguishable from the last.

`config/initializers/live_events.rb` reads `DynamicSettings.find(tree: :private)`, so the `live_events.yml` block has to live under `development.private.canvas` in `dynamic_settings.yml`, not `development.config.canvas`, which is where the checkout ships a stub pointing at a local kinesalite endpoint. That `config:` block is dead config the initializer never reads; `streaming_canvas.py` leaves it alone and writes `kinesis_stream_name: canvas`, `aws_region: us-east-1`, and the two keys from `.canvas-live-events.env` as `aws_access_key_id` and `aws_secret_access_key_dec` under the `private` tree instead. It's idempotent: a rerun with unchanged keys is a no-op and skips the restart. `.canvas-live-events.env` holds a live secret, so `streaming.py` adds it to `.gitignore` before writing it.

Everything is named `canvas` where the service allows one name.

| Resource | Name and shape |
|---|---|
| Kinesis data stream | `canvas`, provisioned, 1 shard, 24 h retention |
| S3 bucket | `canvas-live-events-772750082228`, prefix `raw/live-events/`, errors under `errors/` |
| Firehose delivery stream | `canvas`, source is the stream, 60 s or 1 MB buffer, GZIP |
| IAM role for Firehose | `canvas-firehose` |
| IAM role for Redshift | `canvas-redshift-kinesis`, attached to the namespace as its default role |
| IAM user for Canvas | `canvas-live-events`, PutRecord on the stream and nothing else |
| Redshift objects | external schema `kinesis_src`, materialized view `"raw".live_events_mv`, view `"raw".live_events` |

`raw` is a reserved word in Redshift, so every reference to that schema has to be quoted. `select * from "raw".live_events` works and the unquoted spelling is a syntax error.

The materialized view is `AUTO REFRESH NO` on purpose. Auto refresh polls Kinesis continuously and keeps the 4 RPU workgroup billing around the clock. `streaming_refresh.py` runs the `REFRESH` on demand instead, then prints the row count, the 20 most recent events, and the newest objects under the S3 prefix. It is the check to rerun after Canvas emits events.

Cost while nothing is running is the one provisioned shard, $0.015 per shard-hour, about $0.36 a day. Firehose and S3 are under a dollar a month at this volume. Redshift bills only for the seconds a query runs, with a 60-second minimum per burst, so a refresh costs a few cents.

`cleanup.py --yes` removes all of it. It walks `streaming.py`'s resource table backwards, so the delivery stream goes before the stream and the bucket it feeds, and each role goes after the thing that uses it.

## Tags

Every resource carries `Project=canvas` and `Environment=poc`. `Project=canvas` is the key `cleanup.py` lists and deletes by. Anything created later for this POC has to carry it too, or cleanup will walk straight past it and leave it running.

## Access

All SQL goes through the Redshift Data API under the caller's own IAM identity. There is no database password to store or rotate, and no network path into the VPC to arrange. The workgroup is not publicly accessible.
