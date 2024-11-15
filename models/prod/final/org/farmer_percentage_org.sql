{{ config(
  materialized='table'
) }}

SELECT DISTINCT *
FROM {{ ref('farmer_niti_un_percentage') }}

UNION

SELECT DISTINCT *
FROM {{ ref('farmer_gramin_percentage') }}

UNION

SELECT DISTINCT *
FROM {{ ref('farmer_gdgs_un_percentage') }}