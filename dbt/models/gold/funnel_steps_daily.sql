{{ config(
    materialized="incremental",
    incremental_strategy="append",
    on_schema_change="append_new_columns",
    unique_key="event_date"
) }}

with base as (
  select *
  from {{ ref('stg_events') }}
  {% if is_incremental() %}
    where event_date > (select coalesce(max(event_date), date('1900-01-01')) from {{ this }})
  {% endif %}
),

steps as (
  select
    event_date,
    user_id,
    case
      when event_type = 'view' then 'view'
      when event_type = 'cart' then 'cart'
      when event_type = 'purchase' then 'purchase'
      else null
    end as step
  from base
),

agg as (
  select
    event_date,
    count(distinct case when step='view' then user_id end) as users_view,
    count(distinct case when step='cart' then user_id end) as users_cart,
    count(distinct case when step='purchase' then user_id end) as users_purchase
  from steps
  group by event_date
)

select
  event_date,
  users_view,
  users_cart,
  users_purchase,
  case when users_view = 0 then 0.0 else users_cart / users_view end as conv_view_to_cart,
  case when users_cart = 0 then 0.0 else users_purchase / users_cart end as conv_cart_to_purchase,
  case when users_view = 0 then 0.0 else users_purchase / users_view end as conv_view_to_purchase
from agg