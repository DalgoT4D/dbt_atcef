{{ config(
  materialized='table'
) }}

SELECT DISTINCT *,
'Niti Aayog' AS project
FROM {{ ref('machine_niti_union') }}

UNION

SELECT DISTINCT *,
'Project A' AS project
FROM {{ ref('machine_gramin_metric') }}

UNION

SELECT DISTINCT *,
'GDGS' AS project
FROM {{ ref('machine_gdgs_union') }}