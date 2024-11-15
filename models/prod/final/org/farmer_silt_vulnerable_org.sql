{{ config(
  materialized='table'
) }}

SELECT DISTINCT *
FROM {{ ref('farmer_silt_vulnerable_niti_union') }}

UNION

SELECT DISTINCT *
FROM {{ ref('farmer_silt_vulnerable_gramin') }}

UNION

SELECT DISTINCT *
FROM {{ ref('farmer_silt_vulnerable_gdgs_union') }}