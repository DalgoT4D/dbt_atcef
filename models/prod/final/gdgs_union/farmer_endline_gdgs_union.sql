{{ config(
  materialized='table',
  tags=["final","final_gdgs_union"]
) }}

SELECT DISTINCT *
FROM {{ ref('farmer_endline') }}

UNION

SELECT DISTINCT *
FROM {{ ref('farmer_endline_gdgs_2023') }}
