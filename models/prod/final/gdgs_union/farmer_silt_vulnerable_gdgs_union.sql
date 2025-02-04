{{ config(
  materialized='table',
  tags=["final","final_gdgs_union"]
) }}

SELECT *
FROM {{ ref('farmer_silt_vulnerable_gdgs_23') }}

UNION

SELECT *
FROM {{ ref('farmer_silt_vulnerable_gdgs_24') }}
