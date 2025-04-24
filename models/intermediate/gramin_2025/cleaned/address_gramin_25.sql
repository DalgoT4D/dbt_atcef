{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gramin_niti", "gramin_niti", "gramin_25"]
) }}


WITH cte AS (
    SELECT
        "ID" AS address_id,
        "Title" AS dam,
        "customProperties" ->> 'Estimated quantity of Silt' AS silt_target,
        "customProperties"
        ->> 'Stakeholder responsible' AS stakeholder_responsible,
        "Parent" ->> 'Title' AS taluka,
        "Parent" -> 'Parent' ->> 'Title' AS village,
        "Parent" -> 'Parent' -> 'Parent' ->> 'Title' AS district,
        "Parent" -> 'Parent' -> 'Parent' -> 'Parent' ->> 'Title' AS state
    FROM
        {{ source('source_gramin_25', 'address_gramin_25') }}
    WHERE
        "Title" NOT LIKE '%(voided~%)'
)

SELECT * FROM cte
WHERE silt_target IS NOT null
