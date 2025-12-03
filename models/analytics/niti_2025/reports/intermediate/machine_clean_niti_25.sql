{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "analytical_models", "reports_niti_2025"]
) }}


-- Workorder details
with workoderdetails as (
SELECT
w.subject_id as workorderid,
w.silt_to_be_excavated_as_per_plan,
w.state,
w.district,
w.taluka,
w.village,
w.dam,
w.stakeholder_responsible,
w.workorder_first_name as workorder_name,
w.updated_workorder_name,
cast(w.registration_date as TIMESTAMP) as work_order_start_date
from {{ ref('work_order_regn_niti_25') }} as w
WHERE w.approval_status = 'Approved'
),



-- Farmer silt details
farmer_carted as (
SELECT
    wf.farmer_work_order_sub_id as workorderid,
    SUM(CASE
            -- Only sum the silt_carted if the record is approved
            WHEN a.approval_status = 'Approved' THEN COALESCE(wf.silt_carted, 0)
            ELSE 0
END) as total_silt_carted_by_farmers,
    count(distinct wf.farmer_beneficiary_id) as total_number_of_farmers

from {{ ref('work_order_farmer_niti_25') }} as wf

INNER JOIN {{ ref('approval_status_niti_25') }} as a 
    ON wf.eid = a.entity_id 
WHERE wf.voided != TRUE
group by wf.farmer_work_order_sub_id),


-- machine type merge
machine_type as (
SELECT
wf.farmer_work_order_sub_id as workorderid,
wf.silt_carted,
wf.machine_sub_id,
mr.machine_type
from {{ ref('work_order_farmer_niti_25') }} as wf
LEFT JOIN {{ref('machine_regn_niti_25')}} as mr
ON mr.subject_id = wf.machine_sub_id
INNER JOIN {{ ref('approval_status_niti_25') }} as a 
ON wf.eid = a.entity_id ),

machine_type_pivot AS (
    SELECT
        workorderid,
        LOWER(machine_type) AS machine_type_lower,
        SUM(silt_carted) AS total_silt_carted_by_type
    FROM machine_type
    GROUP BY 1, 2),

carting_by_machine_type AS (
    SELECT 
    workorderid,
    SUM(CASE WHEN machine_type_lower = 'jcb' THEN total_silt_carted_by_type ELSE 0 END) AS jcb_excavation,
    SUM(CASE WHEN machine_type_lower = 'poclain' THEN total_silt_carted_by_type ELSE 0 END) AS poclain_excavation
FROM machine_type_pivot
GROUP BY 1),



-- GP silt details
gp_silt as (
    SELECT
    ge.endline_gp_sub_id as workorderid,
    sum(case 
    when a.approval_status = 'Approved' then COALESCE(ge.total_gp_silt_excavated_non_farm, 0) 
    else 0 end) as total_silt_excavated_by_gp_non_farm
    from {{ ref('gp_endline_niti_25') }} as ge
    INNER JOIN {{ ref('approval_status_niti_25') }} as a 
    ON ge.eid = a.entity_id 
    WHERE ge.voided = false 
    GROUP BY ge.endline_gp_sub_id
),

-- Workorder endline silt details
workorder_endline as (
SELECT
we.workorder_id,
we.total_silt_excavated,
cast(we.endline_date as TIMESTAMP) as workorder_endline_date
from {{ ref('workorder_endline_linelist_niti_25') }} as we 
),

-- machine details
machine_silt as (
SELECT 
    mw.machine_work_order_sub_id as workorderid,
    SUM(CASE 
        -- Total working hours (Approved only)
        WHEN a.approval_status = 'Approved' THEN COALESCE(mw.working_hours, 0) 
        ELSE 0 
    END) as total_machine_working_hours,
    SUM(CASE
        -- JCB working hours (Approved only)
        WHEN a.approval_status = 'Approved' 
             AND LOWER(mr.machine_type) = 'jcb' 
             THEN COALESCE(mw.working_hours, 0)
        ELSE 0
    END) as jcb_working_hours,
    SUM(CASE
        -- Poclain working hours (Approved only)
        WHEN a.approval_status = 'Approved' 
             AND LOWER(mr.machine_type) = 'poclain' 
             THEN COALESCE(mw.working_hours, 0)
        ELSE 0
    END) as poclain_working_hours
FROM {{ ref('work_order_machine_niti_25') }} as mw
INNER JOIN {{ ref('approval_status_niti_25') }} as a 
    ON mw.eid = a.entity_id 
-- Add machine registration table to get machine type
LEFT JOIN {{ ref('machine_regn_niti_25') }} AS mr
    ON mw.excavating_machine_id = mr.subject_id
WHERE mw.voided != TRUE
GROUP BY mw.machine_work_order_sub_id
),

--machine types
machine_types_counted as (
SELECT
    t.workorderid,
    SUM(CASE WHEN t.machine_type_lower = 'jcb' THEN 1 ELSE 0 END) AS jcb_count,
    SUM(CASE WHEN t.machine_type_lower = 'poclain' THEN 1 ELSE 0 END) AS poclain_count
FROM
    (SELECT DISTINCT
            mw.machine_work_order_sub_id AS workorderid,
            mw.excavating_machine_id,
            LOWER(mr.machine_type) AS machine_type_lower
        FROM {{ ref('work_order_machine_niti_25') }} AS mw
        LEFT JOIN{{ ref('machine_regn_niti_25') }} AS mr
            ON mw.excavating_machine_id = mr.subject_id
        WHERE mw.voided != TRUE AND mr.approval_status = 'Approved') AS t
GROUP BY t.workorderid)


-- FINAL TABLE

SELECT
    wd.workorderid,
    wd.workorder_name,
    wd.updated_workorder_name,
    wd.state,
    wd.district,
    wd.taluka,
    wd.village,
    wd.dam,
    wd.stakeholder_responsible,
    wd.silt_to_be_excavated_as_per_plan,
    fc.total_silt_carted_by_farmers,
    gs.total_silt_excavated_by_gp_non_farm,
    we.total_silt_excavated,
    ms.total_machine_working_hours,
    mtc.poclain_count,
    mtc.jcb_count,
    fc.total_number_of_farmers,
    wd.work_order_start_date,
    we.workorder_endline_date,
    ms.jcb_working_hours,
    ms.poclain_working_hours,
    exc.jcb_excavation,
    exc.poclain_excavation
FROM
    workoderdetails AS wd
LEFT JOIN
    farmer_carted AS fc ON wd.workorderid = fc.workorderid
LEFT JOIN
    gp_silt AS gs ON wd.workorderid = gs.workorderid
LEFT JOIN
    workorder_endline AS we ON wd.workorderid = we.workorder_id
LEFT JOIN
    machine_silt AS ms ON wd.workorderid = ms.workorderid
LEFT JOIN
    machine_types_counted AS mtc ON wd.workorderid = mtc.workorderid
LEFT JOIN
carting_by_machine_type AS exc ON wd.workorderid = exc.workorderid

