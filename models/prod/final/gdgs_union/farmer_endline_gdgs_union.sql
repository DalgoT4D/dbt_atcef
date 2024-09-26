{{ config(
  materialized='table'
) }}

SELECT DISTINCT *
FROM {{ ref('farmer_endline') }}

UNION

SELECT DISTINCT *
FROM {{ ref('farmer_endline_gdgs_2023') }}
