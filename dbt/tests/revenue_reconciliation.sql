with silver as (
  select coalesce(sum(price), 0) as revenue, count(*) as events
  from {{ ref('stg_events') }} where event_type='purchase'
), gold as (
  select coalesce(sum(revenue), 0) as revenue, coalesce(sum(purchase_events), 0) as events
  from {{ ref('daily_sales_by_category') }}
)
select * from silver s cross join gold g
where s.revenue<>g.revenue or s.events<>g.events
