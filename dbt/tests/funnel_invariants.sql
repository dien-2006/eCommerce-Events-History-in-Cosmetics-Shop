select * from {{ ref('funnel_steps_daily') }}
where sessions_total<sessions_view or sessions_view<sessions_cart_after_view
   or sessions_cart_after_view<sessions_purchase_after_cart
   or conv_view_to_cart not between 0 and 1
   or conv_cart_to_purchase not between 0 and 1
   or conv_view_to_purchase not between 0 and 1
