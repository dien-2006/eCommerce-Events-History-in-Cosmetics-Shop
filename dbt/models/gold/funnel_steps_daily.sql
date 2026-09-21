{{ config(partition_by=['event_date']) }}
-- Ordered session funnel, attributed to the first observed event date.
with sessions as (
  select user_id, user_session, min(event_date) as event_date,
         min(case when event_type='view' then event_ts end) as view_ts
  from {{ ref('stg_events') }} group by user_id, user_session
), carts as (
  select s.user_id, s.user_session, s.event_date, s.view_ts,
         min(e.event_ts) as cart_ts
  from sessions s left join {{ ref('stg_events') }} e
    on s.user_id=e.user_id and s.user_session=e.user_session
    and e.event_type='cart' and e.event_ts>=s.view_ts
  group by s.user_id, s.user_session, s.event_date, s.view_ts
), purchases as (
  select c.user_id, c.user_session, c.event_date, c.view_ts, c.cart_ts,
         min(e.event_ts) as purchase_ts
  from carts c left join {{ ref('stg_events') }} e
    on c.user_id=e.user_id and c.user_session=e.user_session
    and e.event_type='purchase' and e.event_ts>=c.cart_ts
  group by c.user_id, c.user_session, c.event_date, c.view_ts, c.cart_ts
), daily as (
  select event_date, count(*) as sessions_total, count(view_ts) as sessions_view,
         count(cart_ts) as sessions_cart_after_view,
         count(purchase_ts) as sessions_purchase_after_cart
  from purchases group by event_date
)
select *,
  coalesce(sessions_cart_after_view / nullif(sessions_view, 0), 0.0) as conv_view_to_cart,
  coalesce(sessions_purchase_after_cart / nullif(sessions_cart_after_view, 0), 0.0) as conv_cart_to_purchase,
  coalesce(sessions_purchase_after_cart / nullif(sessions_view, 0), 0.0) as conv_view_to_purchase
from daily
