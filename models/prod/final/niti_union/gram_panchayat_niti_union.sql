{{ config(
  materialized='table',
  tags=["final","final_niti_union", "niti"]
) }}

SELECT *
FROM {{ ref('gram_panchayat_niti_23') }}
