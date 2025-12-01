{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti"]
) }}

SELECT


from
{{ ref('dim_subjects_work_order_niti_25') }} as ws
left join
{{ ref('work_order_endline_niti_25') }} as we
on
ws.subject_id = we.endline_work_order_sub_id