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
        st_nm,
        MAX(geojson) AS geojson -- Assuming selection of a single geojson representation is okay
    FROM
        public.state_jeojson as sj
    GROUP BY
        st_nm
) AS sj ON cf.state = sj.st_nm