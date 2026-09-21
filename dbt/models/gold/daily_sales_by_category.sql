{{ config(partition_by=['event_date']) }}
-- No order_id exists. A purchase event is a product line, not an order.
select event_date, category_code, brand,
  cast(sum(price) as decimal(20, 2)) as revenue,
  count(*) as purchase_events,
  count(distinct struct(user_id, user_session)) as purchasing_sessions,
  cast(avg(price) as decimal(20, 4)) as avg_purchase_event_value
from {{ ref('stg_events') }} where event_type = 'purchase'
group by event_date, category_code, brand
