select event_date, category_code, brand, count(*) as n
from {{ ref('daily_sales_by_category') }}
group by event_date, category_code, brand having count(*)>1
