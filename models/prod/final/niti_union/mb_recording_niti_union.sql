{{ config(
  materialized='table',
  tags=["final","final_niti_union", "niti"]
) }}


SELECT
    state,
    district,
    taluka,
    dam,
    ngo_name,
    village,
    max(date_time) AS date_time,
    sum(
        coalesce(silt_excavated_as_per_mb_recording, 0)
    ) AS silt_excavated_as_per_mb_recording,
    max(coalesce(silt_target, 0)) AS total_silt_excavated
FROM
    {{ ref('work_order_niti_union') }}
GROUP BY
    state,
    district,
    taluka,
    dam,
    ngo_name,
    village
