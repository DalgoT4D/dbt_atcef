{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti"]
) }}


Select
ws.workorder_first_name as workorder_name,
ws.updated_workorder_name as updated_workorder_name,
ws.state,
ws.district,
ws.gp_village as village,
ws.taluka,
ws.dam,
ws.ngo_name,



we.encounter_date_time,
we.ngo,
we.site_video,
we.mb_recording_done,
we.site_image_1_url,
we.site_image_2_url,
we.total_silt_excavated,
we.mb_document_url,
we.silt_excavated_as_per_mb,
we.is_mb_data_same_as_app_data

FROM {{ ref('work_order_endline_niti_25') }} as we
Left JOIN {{ ref('dim_subjects_work_order_niti_25') }} AS ws
    ON we.endline_work_order_sub_id = ws.subject_id
