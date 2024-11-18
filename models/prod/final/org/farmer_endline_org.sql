{{ config(
  materialized='table'
) }}

SELECT DISTINCT *,
'Niti Aayog' AS project
FROM {{ ref('farmer_endline_niti_union') }}

UNION

SELECT DISTINCT *,
'Project A' AS project
FROM {{ ref('farmer_endline_gramin') }}

UNION

SELECT DISTINCT *,
'GDGS' AS project
FROM {{ ref('farmer_endline_gdgs_union') }}