{{ config(
  materialized='table',
  tags=["final","final_gdgs_union", "gdgs"]
) }}

SELECT DISTINCT *
FROM {{ ref('machine_endline') }}

UNION

SELECT DISTINCT *
FROM {{ ref('machine_endline_gdgs_2023') }}
