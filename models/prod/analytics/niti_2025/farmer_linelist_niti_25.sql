{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti"]
) }}







-- SELECT
--     farmer_sub_id,
--     approval_status,
--     mobile_verified,
--     first_name as farmer_name,
--             case  -- Standardize state names
--             when
--                 LOWER(state) like '%maharashtra%'
--                 then 'Maharashtra'
--             when
--                 LOWER(state) like '%maharshatra%'
--                 then 'Maharashtra'
--             else INITCAP(COALESCE(state, ''))
--         end as state,
--     district,
--     taluka,
--     village,
--     dam,
--     category_of_farmer,
--     ngo_name,
--     mobile_number,
--     SUM(COALESCE(total_silt_carted, 0)::numeric) AS total_silt_carted_sum,
--     SUM(COALESCE(NULLIF(number_of_trolleys_carted, '')::numeric, 0)) AS number_of_trolleys_carted_sum,
--     MAX(COALESCE(silt_target, 0)::numeric) AS max_silt_target
-- FROM dev_analytics.work_order_daily_recording_farmer -- placeholder to change
-- WHERE COALESCE(voided, FALSE) = FALSE
--   AND COALESCE(subject_voided, FALSE) = FALSE
-- --   AND first_name IS NOT NULL
-- GROUP BY
--     farmer_sub_id,
--     approval_status,
--     mobile_verified,
--     first_name,
--     dam,
--     district,
--     state,
--     taluka,
--     village,
--     category_of_farmer,
--     ngo_name,
--     mobile_number
