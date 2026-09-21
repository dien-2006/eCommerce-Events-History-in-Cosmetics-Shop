with total as (
  select _file_sha256, count(*) as n
  from {{ source('bronze', 'events_history') }} group by _file_sha256
), invalid as (
  select _file_sha256, count(*) as n from {{ ref('quarantine_events') }} group by _file_sha256
)
select t._file_sha256, t.n as total_rows, i.n as invalid_rows
from total t join invalid i on t._file_sha256=i._file_sha256
where i.n / t.n > {{ var('max_invalid_ratio', 0.01) }}
