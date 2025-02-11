{{ config(
  materialized='table',
  tags=["intermediate","intermediate_niti_2022", "niti_2022", "niti"]
) }}


with cte as (SELECT
    "ID" as address_id,
    "Title" AS dam,
    "customProperties"->>'Estimated quantity of Silt' AS silt_target,
    "customProperties"->>'Stakeholder responsible' AS stakeholder_responsible
FROM
    {{ source('source_atecf_surveyss', 'address_niti_2022') }}
WHERE
   "Title" NOT LIKE '%(voided~%')
   
select * from cte 
