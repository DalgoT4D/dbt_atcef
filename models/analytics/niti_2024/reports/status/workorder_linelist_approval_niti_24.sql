-- Combines work order registrations, latest endline info, and contractor details with approval status.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2024",  "analytical_models", "reports_niti_2024"]
) }}


-- Getting machine contractor details and merging with machine registrations
WITH machine_contractors AS (
    SELECT
        wm.machine_work_order_sub_id AS work_order_id,
        mr.contractor_name,
        -- You can add other necessary columns here
        ROW_NUMBER() OVER (
            PARTITION BY wm.machine_work_order_sub_id
            ORDER BY wm.encounter_date_time DESC
        ) AS rn
    FROM {{ ref('work_order_machine_niti_24') }} AS wm
    LEFT JOIN {{ ref('machine_regn_niti_24') }} AS mr
        ON wm.excavating_machine_id = mr.subject_id
),

contractor_info as (SELECT
    work_order_id,
    contractor_name
FROM machine_contractors
WHERE rn = 1)



SELECT
ws.workorder_first_name as workorder_name,
ws.updated_workorder_name as updated_workorder_name,
ws.registration_date,

ws.state,
ws.district,
ws.village,
ws.taluka,
ws.dam,
ws.gram_panchayat_name as gp,

ws.silt_to_be_excavated_as_per_plan,
ws.stakeholder_responsible,

we.encounter_date_time as endline_date,

ws.site_image_1_url,
ws.site_image_2_url,
ws.site_video_url,
ws.noc_workorder_image, -- new
ws.site_marking_image, -- new



mc.contractor_name,
ws.approval_status


from
{{ ref('work_order_regn_niti_24') }} as ws
left join
{{ ref('work_order_endline_niti_24') }} as we -- only take the latest instance of work_order_endline - to be coded in later
on
ws.subject_id = we.endline_work_order_sub_id
left join
contractor_info as mc
on
ws.subject_id = mc.work_order_id
-- where ws.approval_status = 'Approved'