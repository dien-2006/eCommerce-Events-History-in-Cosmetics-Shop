{{ config(
    unique_key="event_dedup_key",
    incremental_strategy="append",
    on_schema_change="append_new_columns"
) }}

with src as (
    select
        -- raw columns
        event_time,
        event_type,
        product_id,
        category_id,
        category_code,
        brand,
        price,
        user_id,
        user_session,
        -- schema evolution safe: may be null if not present in older data
        payment_method,
        _ingestion_time,
        _source_file,
        _batch_id
    from {{ source('cosmetics_bronze', 'events_history') }}
    {% if is_incremental() %}
      where _batch_id not in (select distinct _batch_id from {{ this }})
    {% endif %}
),

typed as (
    select
        to_timestamp(event_time) as event_ts,
        cast(to_date(to_timestamp(event_time)) as date) as event_date,

        -- standardize enum (future-proof)
        lower(trim(event_type)) as event_type,

        cast(product_id as bigint) as product_id,
        cast(category_id as bigint) as category_id,
        lower(trim(category_code)) as category_code,
        lower(trim(brand)) as brand,

        cast(price as double) as price,
        cast(user_id as bigint) as user_id,
        cast(user_session as string) as user_session,

        cast(payment_method as string) as payment_method,

        to_timestamp(_ingestion_time) as _ingestion_time,
        _source_file,
        _batch_id,

        -- dedup key
        sha2(concat_ws(
          '||',
          cast(user_id as string),
          cast(user_session as string),
          cast(product_id as string),
          cast(to_timestamp(event_time) as string)
        ), 256) as event_dedup_key
    from src
),

deduped as (
    select *
    from (
        select
          *,
          row_number() over (
            partition by event_dedup_key
            order by _ingestion_time desc
          ) as rn
        from typed
    ) t
    where rn = 1
)

select
  event_ts,
  event_date,
  event_type,
  product_id,
  category_id,
  category_code,
  brand,
  price,
  user_id,
  user_session,
  payment_method,
  _ingestion_time,
  _source_file,
  _batch_id,
  event_dedup_key
from deduped