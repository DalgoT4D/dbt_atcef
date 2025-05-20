{{ config(
  materialized='table',
  tags=["glific"]
) }}

select 
    id,
    name,
    CAST(inserted_at AS TIMESTAMPTZ)::DATE AS inserted_date,
    CAST(updated_at AS TIMESTAMPTZ)::DATE AS updated_date
from {{ source('staging_glific', 'flow_results_stg') }}

