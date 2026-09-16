CREATE EXTERNAL SCHEMA IF NOT EXISTS kinesis_src
FROM KINESIS
IAM_ROLE '<role arn>';

CREATE SCHEMA IF NOT EXISTS "raw";

CREATE MATERIALIZED VIEW "raw".live_events_mv AUTO REFRESH NO AS
SELECT approximate_arrival_timestamp,
       partition_key,
       shard_id,
       sequence_number,
       refresh_time,
       JSON_PARSE(from_varbyte(kinesis_data, 'utf-8')) AS event
FROM kinesis_src."canvas";

CREATE OR REPLACE VIEW "raw".live_events AS
SELECT event.attributes.event_name::varchar              AS event_name,
       event.attributes.event_time::varchar::timestamp   AS event_time,
       event.attributes.user_id::varchar                 AS user_id,
       event.attributes.root_account_id::varchar         AS root_account_id,
       event.attributes.producer::varchar                AS producer,
       event.attributes.hostname::varchar                AS hostname,
       event.attributes.request_id::varchar              AS request_id,
       event.body                                        AS body,
       approximate_arrival_timestamp,
       shard_id,
       sequence_number
FROM "raw".live_events_mv
WITH NO SCHEMA BINDING;
