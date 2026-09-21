{% macro analysis_date() %}
  {% if var('as_of_date', none) %}
    cast('{{ var("as_of_date") }}' as date)
  {% else %}
    (select max(event_date) from {{ ref('stg_events') }})
  {% endif %}
{% endmacro %}
