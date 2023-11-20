{% test subrogated_key (model, column_name) %}
  select 
    {{ column_name }},
    count(*) as count
  from {{ model }}
  group by {{ column_name }}
  having count(*) > 1
{% endtest %}
