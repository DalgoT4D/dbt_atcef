{{ config(
  materialized='table',
  tags=["glific"]
) }}

select 
    id,
    body,
    topic,
    status,
    flow_id,
    flow_name,
    CAST(inserted_at AS TIMESTAMPTZ)::DATE AS inserted_date,
    CAST(updated_at AS TIMESTAMPTZ)::DATE AS updated_date
from {{ source('staging_glific', 'tickets_stg') }}

