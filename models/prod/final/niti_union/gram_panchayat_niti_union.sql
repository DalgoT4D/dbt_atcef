{{ config(
  materialized='table',
  tags=["final","final_niti_union"]
) }}

SELECT *
FROM {{ ref('gram_panchayat_niti_23') }}

