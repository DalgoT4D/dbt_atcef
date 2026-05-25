{{ config(
  materialized='table',
  tags=["final","final_org"]
) }}

SELECT DISTINCT
    *,
    'Niti Aayog' AS project
FROM {{ ref('farmer_silt_vulnerable_niti_union') }}

UNION

SELECT DISTINCT
    *,
    'Project A' AS project
FROM {{ ref('farmer_silt_vulnerable_gramin_union') }}

UNION

SELECT DISTINCT
    *,
    'GDGS' AS project
FROM {{ ref('farmer_silt_vulnerable_gdgs_union') }}
