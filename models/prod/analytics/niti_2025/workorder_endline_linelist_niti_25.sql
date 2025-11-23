{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti"]
) }}


Select
w.work_order_name,
-- e."ID" as encounter_id,
-- e."Subject_ID" as subject_id,
w.date_time as work_order_date_time,
e."Encounter_date_time" as endline_date,
w.state,
w.district,
w.taluka,
w.village,
w.dam,
w.silt_target,
e.observations ->> 'MB Recording done' as mb_recording,
e.observations ->> 'Silt excavated as per MB recording' as silt_excavated_mb,
e.observations ->> 'Total silt excavated' as total_silt_excavated, -- Check, this to be zero sometimes
e.observations ->> 'NGO Name' as stakeholder_responsible,
e.observations ->> 'Image 1 of the site' as image_1_of_site,
e.observations ->> 'Image 2 of the site' as image_2_of_site,
e.observations ->> 'Document of MB recording' as mb_recording_document

From {{ source('rwb_niti_2025', 'encounters_niti_2025') }} as e

inner join {{ ref('work_order_niti_2025') }} as w
on
e."Subject_ID" = w.work_order_id
Where e."Encounter_type" = 'Work order endline' -- work_order_2025_niti extras here not in this table

