{{ config(
  materialized='table'
) }}

SELECT *
FROM {{ ref('gram_panchayat_niti_23') }}

