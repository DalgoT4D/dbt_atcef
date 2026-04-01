-- Projects waterbody attributes from location_niti_24 (dam, hierarchy, silt target, GPS).
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2024",  "analytical_models", "reports_niti_2024"]
) }}

SELECT 
l.dam,
l.state,
l.district,
l.taluka,
l.village,
l.silt_target as estimated_silt_quantity,
l.site_gps_latitude,
l.site_gps_longitude,
l.gram_panchayat_name,
l.stakeholder_responsible
from {{ ref('location_niti_24') }} as l
