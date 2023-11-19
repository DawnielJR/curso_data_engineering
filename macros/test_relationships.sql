{% macro test_primary_key(model, field) %}
  select 
    {{ field }},
    count(*) as count
  from {{ model }}
  group by {{ field }}
  having count(*) > 1
{% endmacro %}
