{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2023", "analytical_models", "reports_gdgs_2023", "cleaned_gdgs_23"]
) }}

-- ANALYTICAL TABLE: Work Orders × Farmers × Machines × Silt (gdgs 2023)
--
-- ------------------------------------------------------------
-- CTE OVERVIEW WITH SOURCE TABLES
-- ------------------------------------------------------------
-- 1. machine_type: Maps farmer-reported silt carting records to machine types.
--    SOURCES:
--      - work_order_farmer_gdgs_23
--      - machine_regn_gdgs_23
--      - approval_status_gdgs_23
--
-- 2. machine_type_pivot: Aggregates silt carted per work order by machine type.
--    SOURCES:
--      - machine_type (CTE)
--
-- 3. carting_by_machine_type: Produces work-order–level silt excavation totals split by
--      JCB and Poclain.
--    SOURCES:
--      - machine_type_pivot (CTE)
-- 4. machine_silt: Aggregates approved machine working hours per work order,
--      including total hours and hours by machine type.
--    SOURCES:
--      - work_order_machine_gdgs_23
--      - approval_status_gdgs_23
--      - machine_regn_gdgs_23
--
-- 5. machine_types_counted: Counts distinct approved machines used per work order,
--      split by machine type (JCB / Poclain).
--      Also derives ACTIVE machine counts based on machine endlines.
--    SOURCES:
--      - work_order_machine_gdgs_23
--      - machine_regn_gdgs_23
--      - machine_endline_linelist_gdgs_23
--
-- 6. approved_work_orders: Filters to approved work orders and selects core metadata
--      such as location, stakeholder, planned silt, and start date.
--    SOURCES:
--      - work_order_regn_gdgs_23
--
-- 7. farmer_silt: Aggregates farmer-extracted silt and farmer counts per work order,
--      deriving active farmers (> 0 approved silt) and total farmers.
--      SOURCES:
--        - work_order_farmer_gdgs_23
--        - approval_status_gdgs_23

-- 8. farmer_silt_cl: Filters farmer_silt to work orders with non-zero farmer silt.
--      SOURCES:
--        - farmer_silt (CTE)
--
-- 11. gp_silt: Aggregates GP / non-farmer silt excavation per work order.
--     SOURCES:
--       - gp_endline_gdgs_23
--       - approval_status_gdgs_23
--
-- ------------------------------------------------------------
-- FINAL OUTPUT
-- ------------------------------------------------------------
-- ONE ROW PER WORK ORDER, joining:
--   - approved_work_orders
--   - farmer_silt_cl
--   - gp_silt
--   - workorder_endline_linelist_gdgs_23
--   - machine_silt
--   - machine_types_counted
--   - carting_by_machine_type
--
-- ------------------------------------------------------------
-- ACTIVITY DEFIgdgsONS
-- ------------------------------------------------------------
-- Active Work Order: workorder_endline_date IS NULL
-- Active Farmer: farmer has approved silt carted > 0
-- Active Machine: workorder endline NOT reached (to check if this should be revised to machine endline)





-- machine type merge
with machine_type as (
SELECT
wf.farmer_work_order_sub_id as workorderid,
wf.silt_carted,
wf.machine_sub_id,
mr.machine_type
from {{ ref('work_order_farmer_gdgs_23') }} as wf
LEFT JOIN {{ref('machine_regn_gdgs_23')}} as mr
ON mr.subject_id = wf.machine_sub_id
INNER JOIN {{ ref('approval_status_gdgs_23') }} as a 
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
FROM {{ ref('work_order_machine_gdgs_23') }} as mw
INNER JOIN {{ ref('approval_status_gdgs_23') }} as a 
    ON mw.eid = a.entity_id 
-- Add machine registration table to get machine type
LEFT JOIN {{ ref('machine_regn_gdgs_23') }} AS mr
    ON mw.excavating_machine_id = mr.subject_id
WHERE mw.voided != TRUE
GROUP BY mw.machine_work_order_sub_id
),

--machine types
-- machine_types_counted as (
-- SELECT
--     t.workorderid,
--     SUM(CASE WHEN t.machine_type_lower = 'jcb' THEN 1 ELSE 0 END) AS jcb_count,
--     SUM(CASE WHEN t.machine_type_lower = 'poclain' THEN 1 ELSE 0 END) AS poclain_count
-- FROM
--     (SELECT DISTINCT
--             mw.machine_work_order_sub_id AS workorderid,
--             mw.excavating_machine_id,
--             LOWER(mr.machine_type) AS machine_type_lower
--         FROM {{ ref('work_order_machine_gdgs_23') }} AS mw
--         LEFT JOIN{{ ref('machine_regn_gdgs_23') }} AS mr
--             ON mw.excavating_machine_id = mr.subject_id
--         WHERE mw.voided != TRUE AND mr.approval_status = 'Approved') AS t
-- GROUP BY t.workorderid),


machine_types_counted AS (
SELECT
    t.workorderid,
    /* total machines */
    SUM(CASE WHEN t.machine_type_lower = 'jcb' THEN 1 ELSE 0 END) AS jcb_count,
    SUM(CASE WHEN t.machine_type_lower = 'poclain' THEN 1 ELSE 0 END) AS poclain_count,

    /* active machines (endline NOT reached) */
    SUM(
        CASE
            WHEN t.machine_type_lower = 'jcb'
            AND me.endline_date_time IS NULL
            THEN 1 ELSE 0
        END
    ) AS active_jcb_count,

    SUM(
        CASE
            WHEN t.machine_type_lower = 'poclain'
            AND me.endline_date_time IS NULL
            THEN 1 ELSE 0
        END
    ) AS active_poclain_count

FROM (
    SELECT DISTINCT
        mw.machine_work_order_sub_id AS workorderid,
        mw.excavating_machine_id AS machine_id,
        LOWER(mr.machine_type) AS machine_type_lower
    FROM {{ ref('work_order_machine_gdgs_23') }} AS mw
    LEFT JOIN {{ ref('machine_regn_gdgs_23') }} AS mr
        ON mw.excavating_machine_id = mr.subject_id
    WHERE
        mw.voided != TRUE
        AND mr.approval_status = 'Approved'
) t
LEFT JOIN {{ ref('machine_endline_linelist_gdgs_23') }} me
    ON t.machine_id = me.machine_id
GROUP BY t.workorderid
),


-- APPROVED WORK ORDERS

approved_work_orders AS (
    SELECT
        w.subject_id AS workorderid,
        w.silt_to_be_excavated_as_per_plan,
        w.state,
        w.district,
        w.taluka,
        w.village,
        w.dam,
        w.stakeholder_responsible,
        w.workorder_first_name AS workorder_name,
        w.updated_workorder_name,
        CAST(w.registration_date AS TIMESTAMP) AS work_order_start_date
    FROM {{ ref('work_order_regn_gdgs_23') }} AS w
    WHERE w.approval_status = 'Approved'
),

-- FARMER SILT

farmer_level_silt AS (
    SELECT
        wf.farmer_work_order_sub_id AS workorderid,
        wf.farmer_beneficiary_id,
        SUM(
            CASE
                WHEN a.approval_status = 'Approved' THEN COALESCE(wf.silt_carted, 0)
                ELSE 0
            END
        ) AS approved_silt_carted
    FROM {{ ref('work_order_farmer_gdgs_23') }} AS wf
    INNER JOIN {{ ref('approval_status_gdgs_23') }} AS a
        ON wf.eid = a.entity_id
    WHERE wf.voided != TRUE
    GROUP BY
        wf.farmer_work_order_sub_id,
        wf.farmer_beneficiary_id
),

farmer_silt AS (
    SELECT
        workorderid,
        SUM(approved_silt_carted) AS total_silt_carted_by_farmers,
        COUNT(DISTINCT CASE WHEN approved_silt_carted > 0 THEN farmer_beneficiary_id END) AS active_farmers,
        COUNT(DISTINCT farmer_beneficiary_id) AS total_number_of_farmers
    FROM farmer_level_silt
    GROUP BY workorderid
),

farmer_silt_cl as (Select * from farmer_silt where total_silt_carted_by_farmers > 0)


-------------------------------
-------------------------------
-- FINAL TABLE

SELECT DISTINCT

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
    NULL::numeric as total_silt_excavated_by_gp_non_farm, -- added for union compatibility
    (COALESCE(fc.total_silt_carted_by_farmers, 0)
    + COALESCE(NULL::numeric, 0)) AS total_silt_carted_nonendline,
    we.total_silt_excavated,
    ms.total_machine_working_hours,
    mtc.poclain_count,
    mtc.jcb_count,
    fc.total_number_of_farmers,
    wd.work_order_start_date,
    we.endline_date as workorder_endline_date,
    ms.jcb_working_hours,
    ms.poclain_working_hours,
    exc.jcb_excavation,
    exc.poclain_excavation,

    /* ---------------- ACTIVE METRICS ---------------- */

    COALESCE(fc.active_farmers, 0) AS active_farmers,

    -- mtc.active_jcb_count as active_jcbs,
    -- mtc.active_poclain_count as active_poclains,

    CASE WHEN we.endline_date IS NULL
        THEN COALESCE(mtc.poclain_count, 0) ELSE 0
    END AS active_poclains,

    CASE WHEN we.endline_date IS NULL
        THEN COALESCE(mtc.jcb_count, 0) ELSE 0
    END AS active_jcbs,

    CASE
        WHEN we.endline_date IS NULL THEN 1
        ELSE 0
    END AS active_work_orders,

    CASE WHEN we.endline_date IS NULL 
    OR wd.work_order_start_date IS NULL THEN 0
    ELSE (we.endline_date::date - wd.work_order_start_date::date) END AS active_work_order_days

-- FROM approved_work_orders AS wd
-- LEFT JOIN farmer_silt_cl AS fc ON wd.workorderid = fc.workorderid
-- LEFT JOIN {{ref('workorder_endline_linelist_gdgs_23')}} AS we ON wd.workorderid = we.workorder_id
-- LEFT JOIN   machine_silt AS ms ON wd.workorderid = ms.workorderid
-- LEFT JOIN  machine_types_counted AS mtc ON wd.workorderid = mtc.workorderid
-- LEFT JOIN carting_by_machine_type AS exc ON wd.workorderid = exc.workorderid


FROM approved_work_orders AS wd
LEFT JOIN farmer_silt_cl AS fc ON wd.workorderid = fc.workorderid
LEFT JOIN {{ref('workorder_endline_linelist_gdgs_23')}} AS we ON wd.workorderid = we.workorder_id
LEFT JOIN machine_silt AS ms ON wd.workorderid = ms.workorderid
LEFT JOIN machine_types_counted AS mtc ON wd.workorderid = mtc.workorderid
LEFT JOIN carting_by_machine_type AS exc ON wd.workorderid = exc.workorderid
WHERE fc.workorderid IS NOT NULL
