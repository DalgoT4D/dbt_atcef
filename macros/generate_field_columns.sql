{% macro generate_field_columns() %}
  {%- set labels_query -%}
    SELECT DISTINCT field ->> 'label' AS label
    FROM {{ source('staging_glific', 'contacts_field_flatten_stg') }},
         jsonb_array_elements(fields_json::jsonb) AS field
    WHERE field ? 'label'
  {%- endset -%}

  {%- set results = run_query(labels_query) -%}
  {%- if execute -%}
    {%- set rows = results.columns[0].values() -%}
    {%- for label in rows %}
      , MAX(CASE WHEN label = '{{ label }}' THEN value END) AS "{{ label }}"
    {%- endfor %}
  {%- endif %}
{% endmacro %}