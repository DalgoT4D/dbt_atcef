-- Projects waterbody attributes from location_gdgs_25 (dam, hierarchy, silt target, GPS).
{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2025", "gdgs_2025", "gdgs", "analytical_models", "reports_gdgs_2025", "cleaned_gdgs_25"]
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
from {{ ref('location_gdgs_25') }} as l
