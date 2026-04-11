{% macro test_column_positive(model, column_name) %}
{#
  Generic test: assert every non-null value in `column_name` is strictly > 0.
  Usage in schema.yml:
      columns:
        - name: avg_price
          tests:
            - test_column_positive
#}
SELECT
    {{ column_name }} AS failing_value,
    COUNT(*) AS row_count
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }} <= 0
GROUP BY 1
{% endmacro %}
