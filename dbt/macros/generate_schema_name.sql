{#- Use the folder's schema name as is ("gold", "staging") instead of dbt's default "gold_staging". -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
