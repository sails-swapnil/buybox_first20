{% macro flatten_json(model_name, json_column, specific_columns=None) %}

{% if specific_columns is none %}
    {% set json_column_query %}
    SELECT DISTINCT 
        json.key as column_name
    from {{ model_name }},
    lateral flatten(input => PARSE_JSON({{json_column}})) json 
    {% endset %}
    
    {% set results = run_query(json_column_query) %}
    
    {% if execute %}
        {% set results_list = results.columns[0].values() %}
    {% else %}
        {% set results_list = [] %}
    {% endif %}
{% else %}
    {% set results_list = specific_columns %}
{% endif %}

select 
    message_id,
    event_time,
    create_time,
    publish_time, 
    batch_id,

{% for column_name in results_list %}
    {{ json_column }}:{{ column_name }}::variant as {{ column_name }}{% if not loop.last %},{% endif %}
{% endfor %}

from {{ model_name }}

{% endmacro %}
