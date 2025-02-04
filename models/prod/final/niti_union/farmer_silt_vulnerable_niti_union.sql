{{ config(
  materialized='table',
  tags=["final","final_niti_union"]
) }}

SELECT DISTINCT *
FROM {{ ref('farmer_silt_vulnerable_niti_22') }}

UNION

SELECT DISTINCT *
FROM {{ ref('farmer_silt_vulnerable_niti_23') }}

UNION

SELECT DISTINCT *
FROM {{ ref('farmer_silt_vulnerable_niti_24') }}
