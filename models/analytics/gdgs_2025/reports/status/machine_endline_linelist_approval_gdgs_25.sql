-- Joins machine endline encounters with registrations and exposes approval status without filtering it.
{{ config(
  materialized='table',
  tags=["analytics", "analytics_gdgs_2025", "reports_gdgs_2025", "gdgs_25_approval_status"]
) }}


WITH machine_endline_no_void AS (
    SELECT w.*
    FROM {{ ref('machine_endline_analytics_gdgs_25') }} AS w
    WHERE w.voided != TRUE
)


Select
mr.machine_name,
me.endline_machine_sub_id as machine_id,
mr.machine_type,
round(cast(me.total_machine_working_hours as NUMERIC),2) as total_machine_working_hours,
cast(me.encounter_date_time as TIMESTAMP) as endline_date_time,
mr.state,
mr.district,
mr.taluka,
mr.village,
mr.dam,
mr.gram_panchayat_name as gp,
a.approval_status
from 
-- {{ ref('machine_endline_analytics_gdgs_25') }} as me
machine_endline_no_void as me
INNER JOIN {{ ref('machine_regn_gdgs_25') }} as mr
ON mr.subject_id = me.endline_machine_sub_id
LEFT JOIN {{ ref('approval_status_gdgs_25') }} as a
ON a.entity_id = me.eid
-- WHERE me.endline_machine_sub_id IS NOT NULL
-- WHERE a.approval_status = 'Approved'

