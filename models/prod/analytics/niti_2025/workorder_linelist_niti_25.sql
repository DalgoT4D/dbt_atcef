{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti"]
) }}

SELECT
ws.workorder_first_name as workorder_name,
ws.updated_workorder_name as updated_workorder_name,
ws.registration_date,

we.encounter_date_time as endline_date,
ws.state,
ws.district,
ws.gp_village as village,
ws.taluka,
ws.dam,
ws.ngo_name,

ws.silt_to_be_excavated_as_per_plan,
ws.site_image_1_url,
ws.site_image_2_url,
ws.site_video_url,


from
{{ ref('dim_subjects_work_order_niti_25') }} as ws
left join
{{ ref('work_order_endline_niti_25') }} as we
on
ws.subject_id = we.endline_work_order_sub_id