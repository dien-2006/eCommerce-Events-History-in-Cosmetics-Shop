{{ config(materialized='table') }}
select * from {{ ref('int_events_classified') }} where rejection_reason is not null
