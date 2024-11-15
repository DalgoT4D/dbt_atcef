{{ config(
  materialized='table'
) }}

SELECT DISTINCT *
FROM {{ ref('farmer_calc_silt_niti_union') }}

UNION

SELECT DISTINCT *
FROM {{ ref('farmer_calc_silt_gramin') }}

UNION

SELECT DISTINCT *
FROM {{ ref('farmer_calc_silt_gdgs_union') }}