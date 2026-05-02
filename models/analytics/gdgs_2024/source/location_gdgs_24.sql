-- Location dimension derived from address_niti_2024 that rolls up dam, 
-- village-taluka-district hierarchy, stakeholder info, and GPS plus silt target metrics.
  {{ config(
    materialized='table',
    tags=["analytics", "analytics_gdgs_2024", "source", "source_cleaned_gdgs_2024"]
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
        -- "Parent" -> 'Parent' -> 'Parent' -> 'Parent' ->> 'Title' AS state,
        CASE
          WHEN "Parent" -> 'Parent' -> 'Parent' -> 'Parent' ->> 'Title' IN ('Maharshtra', 'Maharashtra')
            THEN 'Maharashtra'
            ELSE "Parent" -> 'Parent' -> 'Parent' -> 'Parent' ->> 'Title'
        END AS state,

        "customProperties" ->> 'Name of Gram Panchayat' AS gram_panchayat_name
        -- CAST("customProperties" ->> 'Estimated quantity of Silt' AS NUMERIC) AS estimated_silt_quantity,
        -- CAST("customProperties" ->> 'GPS Coordinates of the site, Latitude' AS NUMERIC) AS site_gps_latitude,
        -- CAST("customProperties" ->> 'GPS Coordinates of the site, Longitude' AS NUMERIC) AS site_gps_longitude
    FROM
        {{ source('source_gdgsom_surveys', 'address_gdgs_2024') }}
    WHERE
        "Title" NOT LIKE '%(voided~%)'
)

SELECT * FROM cte
WHERE silt_target IS NOT null
