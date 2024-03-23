{{ config(
  materialized='table'
) }}



SELECT
    cf.*,
    sj.geojson
FROM
    {{ ref('category_farmer') }} AS cf
JOIN (
    SELECT
        dtname,
        MAX(geojson) AS geojson -- Assuming selection of a single geojson representation is okay
    FROM
        public.fips_geojson as sj
    GROUP BY
        dtname
) AS sj ON cf.district = sj.dtname