{{ config(
  materialized='table',
  tags=["analytics", "analytics_gramin_2026", "ss_2026", "ss_graminpa_2026"]
) }}

-- Farmer-carted silt plus GP excavation for non-farm use.
SELECT
    work_order_name,
    state,
    district,
    taluka,
    dam,
    ngo_name,
    village,
    last_updated AS date_time,
    silt_target,
    total_silt_achieved AS silt_achieved,
    total_farm_area_with_silt AS total_farm_area_silt_is_spread_on
FROM {{ ref('ss_combined_silt_graminpa_26') }}
