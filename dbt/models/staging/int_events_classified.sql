{{ config(materialized='ephemeral') }}

with typed as (
  select *,
    try_cast(regexp_replace(event_time, ' UTC$', '') as timestamp) as event_ts,
    lower(trim(event_type)) as normalized_event_type,
    try_cast(product_id as bigint) as parsed_product_id,
    try_cast(category_id as bigint) as parsed_category_id,
    try_cast(user_id as bigint) as parsed_user_id,
    try_cast(price as decimal(18, 2)) as parsed_price
  from {{ source('bronze', 'events_history') }}
)
select *, case
    when _corrupt_record is not null then 'malformed_csv'
    when event_ts is null then 'invalid_timestamp'
    when normalized_event_type is null
      or normalized_event_type not in ('view', 'cart', 'remove_from_cart', 'purchase')
      then 'invalid_event_type'
    when parsed_product_id is null or parsed_product_id <= 0 then 'invalid_product_id'
    when parsed_user_id is null or parsed_user_id <= 0 then 'invalid_user_id'
    when user_session is null or trim(user_session) = '' then 'missing_session'
    when parsed_price is null or parsed_price < 0 then 'invalid_price'
    when category_id is not null and parsed_category_id is null then 'invalid_category_id'
    else null
  end as rejection_reason
from typed
