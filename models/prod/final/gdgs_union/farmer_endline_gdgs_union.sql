{{ config(
  materialized='table',
  tags=["final","final_gdgs_union", "gdgs"]
) }}

SELECT DISTINCT *
FROM {{ ref('farmer_endline') }}

UNION

SELECT DISTINCT *
FROM {{ ref('farmer_endline_gdgs_2023') }}
UNION

SELECT DISTINCT *
FROM {{ ref('farmer_endline_gdgs_25') }}
