{{ config(
  materialized='table',
  tags=["glific"]
) }}

WITH deduplicated_source AS (
  {{ 
    dbt_utils.deduplicate(
      relation=source('staging_glific', 'contacts_field_flatten_stg'),
      partition_by='id',
      order_by='updated_at desc'
    ) 
  }}
),

parsed_fields AS (
  SELECT
    c.id AS contact_id,
    c.phone,
    c.inserted_at,
    c.updated_at,
    c.optin_time,
    c.language,
    jsonb_array_elements(c.fields_json::jsonb) AS field
  FROM deduplicated_source AS c
),

flattened AS (
  SELECT
    contact_id,
    phone,
    inserted_at,
    updated_at,
    optin_time,
    language,
    field ->> 'label' AS label,
    field ->> 'value' AS value,
    field ->> 'inserted_at' AS field_inserted_at
  FROM parsed_fields
  WHERE field ? 'label' AND field ? 'value'
),

ranked_fields AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY contact_id, label
      ORDER BY field_inserted_at DESC NULLS LAST
    ) AS rn
  FROM flattened
),

latest_fields AS (
  SELECT * FROM ranked_fields WHERE rn = 1
)

SELECT
  contact_id,
  phone,
  CAST(inserted_at AS TIMESTAMPTZ)::DATE AS inserted_date,
  CAST(updated_at AS TIMESTAMPTZ)::DATE AS updated_date,
  optin_time,
  language
  {{ generate_field_columns() }}
FROM latest_fields
GROUP BY contact_id, phone, inserted_at, updated_at, optin_time, language

