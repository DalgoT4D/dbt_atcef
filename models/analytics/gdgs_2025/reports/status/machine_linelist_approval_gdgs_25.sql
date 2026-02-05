-- Lists machine_regn_gdgs_25 records with contractor and location fields, leaving approval status unfiltered.
{{ config(
  materialized='table',
  tags=["analytics", "gdgs_2025", "gdgs", "analytical_models", "reports_gdgs_2025", "gdgs_25_approval_status"]
) }}

Select
m.machine_name,
m.registration_date, 
m.subject_id as uuid,
m.state, 
m.district, 
m.taluka, 
m.village,
m.gram_panchayat_name as gp,
m.dam,
m.machine_type,
m.contractor_name,
m.contractor_mobile_number,
m.stakeholder_responsible,
m.approval_status

from 
{{ ref('machine_regn_gdgs_25') }} as m
-- WHERE m.approval_status = 'Approved'