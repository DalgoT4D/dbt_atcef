{{ config(
  materialized='table',
  tags=["final","final_gdgs_2023", "gdgs_2023", "gdgs"]
) }}


SELECT
    fc.work_order_name,
    fc.state,
    fc.district,
    fc.taluka,
    fc.dam,
    fc.village,
    fc.ngo_name,
    fe.endline_status,
    MAX(fc.date_time) AS date_time,
    SUM(fc.total_silt_carted) AS silt_achieved_by_endline_farmers,
    SUM(
        fe.total_farm_area_silt_is_spread_on
    ) AS total_farm_area_silt_is_spread_on,
    CASE
        WHEN SUM(fe.total_farm_area_silt_is_spread_on) > 0
            THEN
                ROUND(
                    SUM(fc.total_silt_carted)
                    / NULLIF(SUM(fe.total_farm_area_silt_is_spread_on), 0),
                    2
                )
    END AS silt_per_acre,
    CASE
        WHEN
            SUM(fe.total_farm_area_silt_is_spread_on) > 0
            AND (
                SUM(fc.total_silt_carted)
                / NULLIF(SUM(fe.total_farm_area_silt_is_spread_on), 0)
            )
            >= 420
            THEN 'Above Benchmark'
        ELSE 'Below Benchmark'
    END AS silt_per_acre_benchmark_classification
FROM {{ ref('farmer_calc_silt_gdgs_23') }} AS fc
INNER JOIN {{ ref('farmer_endline_gdgs_2023') }} AS fe
    ON fc.farmer_id = fe.farmer_id
WHERE total_silt_carted != 'NaN' AND endline_status = 'Endline Done'
GROUP BY
    fc.state,
    fc.district,
    fc.taluka,
    fc.dam,
    fc.village,
    fc.work_order_name,
    fc.ngo_name,
    fe.endline_status
