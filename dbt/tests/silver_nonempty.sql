select count(*) as n from {{ ref('stg_events') }} having count(*)=0
