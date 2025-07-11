{{ config(
  materialized='table',
  tags=["intermediate","intermediate_niti_2025", "niti_2025", "niti"]
) }}


WITH cte AS (
    SELECT
        "ID" AS address_id,
        "Title" AS dam,
        "customProperties" ->> 'Estimated quantity of Silt' AS silt_target,
        "customProperties"
        ->> 'Stakeholder responsible' AS stakeholder_responsible,
        "Parent" ->> 'Title' AS village,
        "Parent" -> 'Parent' ->> 'Title' AS taluka,
        "Parent" -> 'Parent' -> 'Parent' ->> 'Title' AS district,
        "Parent" -> 'Parent' -> 'Parent' -> 'Parent' ->> 'Title' AS state
    FROM
        {{ source('rwb_niti_2025', 'address_niti_2025') }}
    WHERE
        "Title" NOT LIKE '%(voided~%)'
)

SELECT * FROM cte
WHERE silt_target IS NOT null
