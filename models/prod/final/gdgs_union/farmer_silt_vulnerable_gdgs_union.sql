{{ config(
  materialized='table'
) }}

SELECT *
FROM {{ ref('farmer_silt_vulnerable_gdgs_23') }}

UNION

SELECT *
FROM {{ ref('farmer_silt_vulnerable_gdgs_24') }}
