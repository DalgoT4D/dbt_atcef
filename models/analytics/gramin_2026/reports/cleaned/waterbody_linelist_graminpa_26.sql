-- Projects waterbody attributes from location_graminpa_26 (dam, hierarchy, silt target, GPS).
{{ config(
  materialized='table',
  tags=["analytics","analytics_gramin_2026", "analytical_models", "reports_graminpa_2026", "cleaned_graminpa_26"]
) }}

SELECT 
l.dam,
l.state,
l.district,
l.taluka,
l.village,
l.silt_target as estimated_silt_quantity,
-- l.site_gps_latitude,
-- l.site_gps_longitude,
l.gram_panchayat_name,
l.stakeholder_responsible,
CAST(NULL AS TIMESTAMP) AS date_time -- Source: none; location model has no registration/encounter date; safe to change/remove.
from {{ ref('location_graminpa_26') }} as l
