{{ config(
    materialized="incremental",
    incremental_strategy="append",
    on_schema_change="append_new_columns",
    unique_key="event_date__category_code"
) }}

with base as (
  select *
  from {{ ref('stg_events') }}
  where event_type = 'purchase'
  {% if is_incremental() %}
    and event_date > (select coalesce(max(event_date), date('1900-01-01')) from {{ this }})
  {% endif %}
),

agg as (
  select
    event_date,
    coalesce(category_code, 'unknown') as category_code,
    sum(price) as revenue,
    count(*) as orders,
    case when count(*) = 0 then 0.0 else sum(price)/count(*) end as aov
  from base
  group by event_date, coalesce(category_code, 'unknown')
)

select
  *,
  concat_ws('__', cast(event_date as string), category_code) as event_date__category_code
from agg