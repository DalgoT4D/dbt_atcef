{{ config(
  materialized='table',
  tags=["glific"]
) }}

with cte as (select 
    id,
    name,
    language,
    phone,
    status,
    user_name,
    user_role,
    CAST(inserted_at AS TIMESTAMPTZ)::DATE AS inserted_date,
    CAST(updated_at AS TIMESTAMPTZ)::DATE AS updated_date
from {{ source('staging_glific', 'contacts_stg') }}) 

{{ dbt_utils.deduplicate(
    relation='cte',
    partition_by='id',
    order_by='updated_date desc',
   )
}}
