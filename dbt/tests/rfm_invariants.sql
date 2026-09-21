select * from {{ ref('rfm_segments') }}
where recency_days<0 or frequency<1 or monetary<0
   or r_score not between 1 and 5 or f_score not between 1 and 5 or m_score not between 1 and 5
