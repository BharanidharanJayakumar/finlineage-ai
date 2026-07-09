{% macro read_csv_source(source_name, table_name) %}
    read_csv_auto('{{ var("bronze_path") }}/{{ table_name }}.csv', header=true)
{% endmacro %}