{{ config(
  materialized='table',
  tags=["final","final_niti_union", "niti"]
) }}

SELECT DISTINCT *
FROM {{ ref('farmer_endline_niti_2022') }}

UNION

SELECT DISTINCT *
FROM {{ ref('farmer_endline_niti_2023') }}

UNION

SELECT DISTINCT *
FROM {{ ref('farmer_endline_niti_2024') }}

UNION

SELECT DISTINCT *
FROM {{ ref('farmer_endline_niti_2025') }}
