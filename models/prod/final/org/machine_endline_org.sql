{{ config(
  materialized='table'
) }}

SELECT DISTINCT *
FROM {{ ref('machine_endline_niti_union') }}

UNION

SELECT DISTINCT *
FROM {{ ref('machine_endline_gramin') }}

UNION

SELECT DISTINCT *
FROM {{ ref('machine_endline_gdgs_union') }}