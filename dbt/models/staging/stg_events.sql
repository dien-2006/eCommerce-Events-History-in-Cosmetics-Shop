{{ config(
    materialized='incremental', incremental_strategy='merge',
    unique_key='event_dedup_key', on_schema_change='fail', partition_by=['event_date']
) }}

with normalized as (
  select event_ts, cast(event_ts as date) as event_date,
    normalized_event_type as event_type,
    parsed_product_id as product_id, parsed_category_id as category_id,
    coalesce(nullif(lower(trim(category_code)), ''), 'unknown') as category_code,
    coalesce(nullif(lower(trim(brand)), ''), 'unknown') as brand,
    parsed_price as price, parsed_user_id as user_id, trim(user_session) as user_session,
    nullif(lower(trim(payment_method)), '') as payment_method,
    _file_sha256, _source_uri, _ingested_at, _run_key
  from {{ ref('int_events_classified') }} where rejection_reason is null
), keyed as (
  select *, sha2(to_json(named_struct(
    'user_id', user_id, 'session', user_session, 'product_id', product_id,
    'event_ts', cast(event_ts as string), 'event_type', event_type, 'price', price
  )), 256) as event_dedup_key from normalized
), ranked as (
  select *, row_number() over (
    partition by event_dedup_key
    order by _ingested_at desc, _file_sha256 desc,
             category_code desc, brand desc, category_id desc nulls last,
             payment_method desc nulls last
  ) as dedup_rank from keyed
)
select event_ts, event_date, event_type, product_id, category_id, category_code, brand,
  price, user_id, user_session, payment_method, event_dedup_key,
  _file_sha256, _source_uri, _ingested_at, _run_key
from ranked where dedup_rank = 1
