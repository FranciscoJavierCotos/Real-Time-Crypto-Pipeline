{% macro test_column_between(model, column_name, min_value, max_value) %}
{#
  Generic test: assert every non-null value in `column_name` is within
  [min_value, max_value] inclusive.
  Usage in schema.yml:
      columns:
        - name: taker_buy_ratio
          tests:
            - test_column_between:
                column_name: taker_buy_ratio
                min_value: 0
                max_value: 1
#}
SELECT
    {{ column_name }} AS failing_value,
    COUNT(*) AS row_count
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND ({{ column_name }} < {{ min_value }} OR {{ column_name }} > {{ max_value }})
GROUP BY 1
{% endmacro %}
