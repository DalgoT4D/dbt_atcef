{{ config(
  materialized='table',
  tags=["glific"]
) }}

select 
    id,
    name,
    language,
    phone,
    status,
    user_name,
    user_role,
    CAST(inserted_at AS TIMESTAMPTZ)::DATE AS inserted_date,
    CAST(updated_at AS TIMESTAMPTZ)::DATE AS updated_date
from {{ source('staging_glific', 'contacts_stg') }}

