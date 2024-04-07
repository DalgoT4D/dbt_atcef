{{ config(
  materialized='table'
) }}


with aggregated_data as (SELECT 
    detailed_data.state,
    detailed_data.district, 
    CAST(SUM(detailed_data.silt_target) AS numeric(10,2)) AS state_silt_target,
    CAST(SUM(detailed_data.silt_achieved) AS numeric(10,2)) AS state_silt_achieved
FROM {{ ref('work_order_metric') }} AS detailed_data
GROUP BY
    detailed_data.district, detailed_data.state),
district_geojson AS (
    SELECT
        dtname,
        MAX(geojson) AS district_geojson
    FROM
        public.fips_geojson
    GROUP BY
        dtname
)

SELECT
    ad.*,
    dj.district_geojson
FROM
    aggregated_data ad
LEFT JOIN district_geojson dj ON ad.district = dj.dtname

