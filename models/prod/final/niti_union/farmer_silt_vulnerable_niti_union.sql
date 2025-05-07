{{ config(
  materialized='table',
  tags=["final","final_niti_union", "niti"]
) }}

SELECT DISTINCT *
FROM {{ ref('farmer_silt_vulnerable_niti_22') }}

UNION

SELECT DISTINCT *
FROM {{ ref('farmer_silt_vulnerable_niti_23') }}

UNION

SELECT DISTINCT *
FROM {{ ref('farmer_silt_vulnerable_niti_24') }}

UNION

SELECT DISTINCT *
FROM {{ ref('farmer_silt_vulnerable_niti_25') }}
