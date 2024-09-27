{{ config(
  materialized='table'
) }}

SELECT DISTINCT *
FROM {{ ref('machine_endline') }}

UNION

SELECT DISTINCT *
FROM {{ ref('machine_endline_gdgs_2023') }}

