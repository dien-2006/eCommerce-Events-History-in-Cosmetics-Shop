{{ config(
    materialized="incremental",
    incremental_strategy="append",
    on_schema_change="append_new_columns",
    unique_key="snapshot_user_key"
) }}

with purchases as (
  select
    user_id,
    event_date,
    price
  from {{ ref('stg_events') }}
  where event_type = 'purchase'
),

snapshot as (
  select cast(current_date() as date) as snapshot_date
),

rfm as (
  select
    s.snapshot_date,
    p.user_id,
    datediff(s.snapshot_date, max(p.event_date)) as recency_days,
    count(*) as frequency,
    sum(p.price) as monetary
  from purchases p
  cross join snapshot s
  group by s.snapshot_date, p.user_id
),

scored as (
  select
    *,
    ntile(5) over (order by recency_days asc) as r_score,
    ntile(5) over (order by frequency desc) as f_score,
    ntile(5) over (order by monetary desc) as m_score
  from rfm
),

segmented as (
  select
    *,
    case
      when r_score >= 4 and f_score >= 4 and m_score >= 4 then 'champions'
      when r_score >= 4 and f_score >= 3 then 'loyal'
      when r_score <= 2 and f_score <= 2 then 'at_risk'
      else 'others'
    end as segment
  from scored
)

select
  *,
  concat_ws('__', cast(snapshot_date as string), cast(user_id as string)) as snapshot_user_key
from segmented
{% if is_incremental() %}
where snapshot_date > (select coalesce(max(snapshot_date), date('1900-01-01')) from {{ this }})
{% endif %}