SELECT 1;

SELECT
  event_date,
  sum(revenue) AS revenue
FROM analytics.daily_sales_by_category
GROUP BY event_date
ORDER BY event_date
LIMIT 10;

SELECT
  segment,
  countDistinct(user_id) AS users
FROM analytics.rfm_segments
GROUP BY segment
ORDER BY users DESC;