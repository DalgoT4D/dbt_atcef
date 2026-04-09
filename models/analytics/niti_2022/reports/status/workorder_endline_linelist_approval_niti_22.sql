-- Keeps the latest work_order_endline_niti_22 per work order and
-- retains the encounter approval status alongside registration context.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2022",  "analytical_models", "reports_niti_2022"]
) }}

-- assuming there may be multiple entries for work order endline 
-- (there were repeats in the farmer endlines)
WITH latest_records AS (
    SELECT
        we.*,
        a.approval_status,
        ROW_NUMBER() OVER (
            PARTITION BY endline_work_order_sub_id
            ORDER BY
                encounter_date_time::timestamp DESC
        ) AS rn
    FROM {{ ref('work_order_endline_niti_22') }} as we
    left join {{ ref('approval_status_niti_22') }} as a
    ON we.eid = a.entity_id
    WHERE voided != TRUE
)


Select
ws.workorder_first_name as workorder_name,
ws.subject_id as workorder_id, -- added
ws.updated_workorder_name as updated_workorder_name,
ws.state,
ws.district,
ws.village,
ws.taluka,
ws.dam,
ws.gram_panchayat_name as gp,

cast(we.encounter_date_time as TIMESTAMP) as endline_date,
we.ngo as stakeholder_responsible,
we.site_video,
we.mb_recording_done,
we.site_image_1_url,
we.site_image_2_url,
we.total_silt_excavated,
we.mb_document_url,
we.silt_excavated_as_per_mb,
we.is_mb_data_same_as_app_data,
we.approval_status

FROM (Select * from latest_records WHERE rn = 1) as we
LEFT JOIN {{ ref('work_order_regn_niti_22') }} AS ws
    ON we.endline_work_order_sub_id = ws.subject_id
-- WHERE ws.approval_status = 'Approved' AND we.approval_status = 'Approved'
-- ONLY returning records with the endline's encounter approval status, not workorder approval status
