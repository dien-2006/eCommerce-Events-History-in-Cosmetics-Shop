with counts as (
  select (select count(*) from {{ source('bronze', 'events_history') }}) as total_rows,
    (select count(*) from {{ ref('quarantine_events') }}) as invalid_rows
)
select * from counts
where total_rows=0 or invalid_rows / total_rows > {{ var('max_invalid_ratio', 0.01) }}
