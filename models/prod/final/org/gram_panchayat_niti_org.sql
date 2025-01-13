{{ config(
  materialized='table'
) }}


SELECT *,
'Niti Aayog' AS project
FROM {{ ref('gram_panchayat_niti_23') }}
