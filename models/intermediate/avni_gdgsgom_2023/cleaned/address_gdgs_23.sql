{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gdgs_2023", "gdgs_2023", "gdgs"]
) }}

WITH cte AS (
    SELECT
        "ID" as address_id,
        "Title" AS dam,
        "customProperties"->>'Estimated quantity of Silt' AS silt_target,
        "customProperties"->>'Stakeholder responsible' AS stakeholder_responsible,
        "Parent"->>'Title' AS taluka,
        "Parent"->'Parent'->>'Title' AS village,
        "Parent"->'Parent'->'Parent'->>'Title' AS district,
        "Parent"->'Parent'->'Parent'->'Parent'->>'Title' AS state
    FROM
        {{ source('source_gdgsom_surveys_2023', 'address_gdgs_2023') }}
    WHERE
        "Title" NOT LIKE '%(voided~%)'
)
SELECT * FROM cte where silt_target is not null
