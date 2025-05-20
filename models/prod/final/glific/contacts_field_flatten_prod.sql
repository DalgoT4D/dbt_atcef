{{ config(
  materialized='table',
  tags=["glific"]
) }}

select 
    id,
    role,
    phone,
    updated_date,
    inserted_date
from {{ ref('contacts_field_flatten_int') }}

