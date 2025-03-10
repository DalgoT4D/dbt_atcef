{{ config(
  materialized='table',
  tags=["final","final_org"]
) }}


SELECT
    *,
    'Niti Aayog' AS project
FROM {{ ref('gram_panchayat_niti_23') }}
