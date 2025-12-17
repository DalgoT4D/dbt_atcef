-- Joins machine endline encounters with registrations and exposes approval status without filtering it.
{{ config(
  materialized='table',
  tags=["analytics", "niti_2025", "niti", "analytical_models", "reports_niti_2025"]
) }}

Select
mr.machine_name,
mr.subject_id as machine_id,
mr.machine_type,
cast(me.total_machine_working_hours as NUMERIC) as total_machine_working_hours,
cast(me.encounter_date_time as TIMESTAMP) as endline_date_time,
mr.state,
mr.district,
mr.taluka,
mr.village,
mr.dam,
mr.gram_panchayat_name as gp,
a.approval_status
from 
{{ ref('machine_endline_niti_25') }} as me
LEFT JOIN {{ ref('machine_regn_niti_25') }} as mr
ON mr.subject_id = me.endline_machine_sub_id
LEFT JOIN {{ ref('approval_status_niti_25') }} as a
ON a.entity_id = me.eid
-- WHERE a.approval_status = 'Approved'

