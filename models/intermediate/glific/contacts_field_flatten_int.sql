{{ config(
  materialized='table',
  tags=["glific"]
) }}

select 
    id,
    "Role" as role,
    phone,
    language,
    optin_time,
    last_message_at,
    CAST(inserted_at AS TIMESTAMPTZ)::DATE AS inserted_date,
    CAST(updated_at AS TIMESTAMPTZ)::DATE AS updated_date
from {{ source('staging_glific', 'contacts_field_flatten_int') }}

