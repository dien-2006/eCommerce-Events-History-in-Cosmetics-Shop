CREATE TABLE IF NOT EXISTS analytics.funnel_steps_daily
(
  event_date Date,
  users_view UInt64,
  users_cart UInt64,
  users_purchase UInt64,
  conv_view_to_cart Float64,
  conv_cart_to_purchase Float64,
  conv_view_to_purchase Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date);

CREATE TABLE IF NOT EXISTS analytics.daily_sales_by_category
(
  event_date Date,
  category_code String,
  revenue Float64,
  orders UInt64,
  aov Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, category_code);

CREATE TABLE IF NOT EXISTS analytics.rfm_segments
(
  snapshot_date Date,
  user_id UInt64,
  recency_days Int32,
  frequency UInt64,
  monetary Float64,
  r_score Int32,
  f_score Int32,
  m_score Int32,
  segment String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (snapshot_date, segment, user_id);