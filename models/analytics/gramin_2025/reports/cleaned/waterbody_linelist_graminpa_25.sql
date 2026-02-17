-- Projects waterbody attributes from location_graminpa_25 (dam, hierarchy, silt target, GPS).
{{ config(
  materialized='table',
  tags=["analytics","analytics_gramin_2025", "analytical_models", "reports_graminpa_2025", "cleaned_graminpa_25"]
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
l.stakeholder_responsible
from {{ ref('location_graminpa_25') }} as l
