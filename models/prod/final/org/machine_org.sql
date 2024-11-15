{{ config(
  materialized='table'
) }}

SELECT DISTINCT *
FROM {{ ref('machine_niti_union') }}

UNION

SELECT DISTINCT *
FROM {{ ref('machine_gramin_metric') }}

UNION

SELECT DISTINCT *
FROM {{ ref('machine_gdgs_union') }}