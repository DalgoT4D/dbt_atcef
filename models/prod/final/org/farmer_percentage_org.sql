{{ config(
  materialized='table',
  tags=["final","final_org"]
) }}

SELECT DISTINCT
    *,
    'Niti Aayog' AS project
FROM {{ ref('farmer_niti_un_percentage') }}

UNION

SELECT DISTINCT
    *,
    'Project A' AS project
FROM {{ ref('farmer_gramin_percentage') }}

UNION

SELECT DISTINCT
    *,
    'GDGS' AS project
FROM {{ ref('farmer_gdgs_un_percentage') }}
