{{ config(
  materialized='table',
  tags=["final","final_gdgs_union", "gdgs"]
) }}

SELECT *
FROM {{ ref('farmer_silt_vulnerable_gdgs_23') }}

UNION

SELECT *
FROM {{ ref('farmer_silt_vulnerable_gdgs_24') }}
