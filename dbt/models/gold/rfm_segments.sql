{{ config(partition_by=['snapshot_date']) }}
with purchases as (
  select * from {{ ref('stg_events') }}
  where event_type='purchase' and event_date <= {{ analysis_date() }}
), rfm as (
  select {{ analysis_date() }} as snapshot_date, user_id,
    datediff({{ analysis_date() }}, max(event_date)) as recency_days,
    count(distinct user_session) as frequency,
    cast(sum(price) as decimal(20, 2)) as monetary
  from purchases group by user_id
), scored as (
  -- Best values receive 5. percent_rank keeps tied metric values together.
  select *,
    cast(5 - floor(4 * percent_rank() over (order by recency_days asc)) as int) as r_score,
    cast(5 - floor(4 * percent_rank() over (order by frequency desc)) as int) as f_score,
    cast(5 - floor(4 * percent_rank() over (order by monetary desc)) as int) as m_score
  from rfm
)
select *, case
  when r_score>=4 and f_score>=4 and m_score>=4 then 'champions'
  when r_score>=3 and f_score>=4 then 'loyal'
  when r_score<=2 and f_score>=3 then 'at_risk'
  when r_score<=2 and f_score<=2 then 'hibernating'
  else 'others' end as segment
from scored
