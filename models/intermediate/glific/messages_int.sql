{{ config(
  materialized='table',
  tags=["glific"]
) }}

select 
    id,
    flow,
    is_hsm,
    contact_phone,
    bsp_status,
    flow_name,
    CAST(inserted_at AS TIMESTAMPTZ)::DATE AS inserted_date,
    CAST(updated_at AS TIMESTAMPTZ)::DATE AS updated_date
from {{ source('staging_glific', 'messages_stg') }}

