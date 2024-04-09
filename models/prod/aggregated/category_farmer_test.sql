{{ config(
  materialized='table'
) }}


WITH aggregated_data AS (
    SELECT 
        dam, 
        MAX(date_time) as date_time,
        district, 
        state, 
        taluka, 
        village, 
        COALESCE(MAX(silt_to_be_excavated), 0) as silt_target, 
        COALESCE(CAST(ROUND(SUM(CASE WHEN total_silt_carted::text <> 'NaN' THEN total_silt_carted ELSE 0 END)) AS numeric), 0) AS silt_achieved
    FROM
        {{ ref('work_order_union') }} 
    WHERE
       dam is not null and date_time is not null
    GROUP BY
        state, district, taluka, village, dam
),
state_geojson AS (
    SELECT
        st_nm,
        MAX(geojson) AS state_geojson
    FROM
        state_jeojson
    GROUP BY
        st_nm
),
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
    sg.state_geojson,
    dj.district_geojson
FROM
    aggregated_data ad
LEFT JOIN state_geojson sg ON ad.state = sg.st_nm
LEFT JOIN district_geojson dj ON ad.district = dj.dtname
