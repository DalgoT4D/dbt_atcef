{{ config(
  materialized='table'
) }}

SELECT DISTINCT *
FROM {{ ref('machine_endline_niti_2022') }}

UNION

SELECT DISTINCT *
FROM {{ ref('machine_endline_niti_2023') }}

UNION

SELECT DISTINCT *
FROM {{ ref('machine_endline_niti_2024') }}