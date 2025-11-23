{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti"]
) }}

Select
w.work_order_id,
w.work_order_name,
w.state,
w.district,
w.taluka,
w.village,
w.dam,
w.ngo_name,
w.silt_target,
ws."Registration_date" as registration_date,
ws.observations ->> 'Image 1 of the site' as image_1_of_site,
ws.observations ->> 'Image 2 of the site' as image_2_of_site,
ws.observations ->> 'Video of Site' as any_other_relevant_image,
ws.observations ->> 'NOC/Work order image' as noc_work_order_image,
ws.observations ->> 'Machine contractor agreement image' as machine_contractor,
ws.observations ->> 'Village institution agreement image (for contribution collection)' as village_institution


from
{{ ref('work_order_niti_2025') }} as w
left join
{{ source('rwb_niti_2025', 'subjects_niti_2025') }} as ws
on
w.work_order_id = ws."ID"