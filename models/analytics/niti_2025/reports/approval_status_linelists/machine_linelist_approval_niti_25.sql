{{ config(
  materialized='table',
  tags=["analytics", "niti_2025", "niti", "analytical_models", "reports_niti_2025"]
) }}

Select
m.machine_name,
m.registration_date, 
m.subject_id as uuid,
m.state, 
m.district, 
m.taluka, 
m.village,
m.dam,
m.machine_type,
m.contractor_name,
m.contractor_mobile_number,
m.stakeholder_responsible,
m.approval_status

from 
{{ ref('machine_regn_niti_25') }} as m
-- WHERE m.approval_status = 'Approved'