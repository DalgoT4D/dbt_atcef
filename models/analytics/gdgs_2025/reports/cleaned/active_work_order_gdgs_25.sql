{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2025", "analytical_models", "reports_gdgs_2025", "cleaned_gdgs_25"]
) }}

-- Work-order-level analytical table for GDGS 2025. Farmer and machine metrics
-- are derived from the approved daily linelists, while GP/non-farm excavation
-- remains a NULL placeholder so this schema stays aligned with the other 2025
-- project marts.

with carting_by_machine_type AS (
    SELECT
        df.work_order_id AS workorderid,
        SUM(CASE WHEN LOWER(mr.machine_type) = 'jcb' THEN COALESCE(df.silt_carted, 0) ELSE 0 END) AS jcb_excavation,
        SUM(CASE WHEN LOWER(mr.machine_type) = 'poclain' THEN COALESCE(df.silt_carted, 0) ELSE 0 END) AS poclain_excavation
    FROM {{ ref('daily_farmer_linelist_gdgs_25') }} AS df
    LEFT JOIN {{ ref('machine_regn_gdgs_25') }} AS mr
        ON mr.subject_id = df.machine_id
    GROUP BY df.work_order_id
),



-- machine details
machine_silt as (
SELECT 
    dm.machine_work_order_sub_id as workorderid,
    SUM(COALESCE(dm.working_hours, 0)) as total_machine_working_hours,
    SUM(CASE WHEN LOWER(mr.machine_type) = 'jcb' THEN COALESCE(dm.working_hours, 0) ELSE 0 END) as jcb_working_hours,
    SUM(CASE WHEN LOWER(mr.machine_type) = 'poclain' THEN COALESCE(dm.working_hours, 0) ELSE 0 END) as poclain_working_hours
FROM {{ ref('daily_machine_linelist_gdgs_25') }} as dm
LEFT JOIN {{ ref('machine_regn_gdgs_25') }} AS mr
    ON dm.machine_id = mr.subject_id
GROUP BY dm.machine_work_order_sub_id
),

machine_types_counted AS (
SELECT
    t.workorderid,
    /* total machines */
    SUM(CASE WHEN t.machine_type_lower = 'jcb' THEN 1 ELSE 0 END) AS jcb_count,
    SUM(CASE WHEN t.machine_type_lower = 'poclain' THEN 1 ELSE 0 END) AS poclain_count,

    /* active machines (endline NOT reached) */
    SUM(CASE WHEN t.machine_type_lower = 'jcb' AND me.endline_date_time IS NULL THEN 1 ELSE 0 END) AS active_jcb_count,
    SUM(CASE WHEN t.machine_type_lower = 'poclain' AND me.endline_date_time IS NULL THEN 1 ELSE 0 END) AS active_poclain_count

FROM (
    SELECT DISTINCT
        dm.machine_work_order_sub_id AS workorderid,
        dm.machine_id AS machine_id,
        LOWER(mr.machine_type) AS machine_type_lower
    FROM {{ ref('daily_machine_linelist_gdgs_25') }} AS dm
    LEFT JOIN {{ ref('machine_regn_gdgs_25') }} AS mr
        ON dm.machine_id = mr.subject_id
    WHERE
        dm.machine_id IS NOT NULL
) t
LEFT JOIN {{ ref('machine_endline_linelist_gdgs_25') }} me
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
    FROM {{ ref('work_order_regn_gdgs_25') }} AS w
    WHERE w.approval_status = 'Approved'
),

farmer_silt_cl as (
    select
        work_order_id,
        count(distinct farmer_id) as total_number_of_farmers,
        sum(silt_carted) as total_silt_carted_by_farmers,
        count(distinct case when silt_carted > 0 then farmer_id end) as active_farmers
    from {{ ref('daily_farmer_linelist_gdgs_25') }}
    group by work_order_id)


-------------------------------
-------------------------------
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

FROM approved_work_orders AS wd
LEFT JOIN farmer_silt_cl AS fc ON wd.workorderid = fc.work_order_id
LEFT JOIN {{ref('workorder_endline_linelist_gdgs_25')}} AS we ON wd.workorderid = we.workorder_id
LEFT JOIN   machine_silt AS ms ON wd.workorderid = ms.workorderid
LEFT JOIN  machine_types_counted AS mtc ON wd.workorderid = mtc.workorderid
LEFT JOIN carting_by_machine_type AS exc ON wd.workorderid = exc.workorderid
