{{ config(
  materialized='table',
  tags=["glific"]
) }}

select 
    id,
    CAST(inserted_at AS TIMESTAMPTZ)::DATE AS inserted_date,
    CAST(updated_at AS TIMESTAMPTZ)::DATE AS updated_date,
    conversation_uuid
from {{ source('staging_glific', 'message_conversations_int') }}

