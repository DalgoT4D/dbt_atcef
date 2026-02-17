{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "analytical_models", "reports_niti_2025"]
) }}

WITH approved_work_orders AS (
    SELECT
        dam,
        updated_workorder_name AS work_order_name,
        subject_id AS work_order_id,
        state,
        village,
        district,
        taluka,
        stakeholder_responsible AS ngo_name,
        registration_date
    FROM {{ ref('work_order_regn_niti_25') }}
    WHERE approval_status = 'Approved'
),

completed_work_orders AS (
    SELECT
        workorder_id AS work_order_id,
        endline_date
    FROM {{ ref('workorder_endline_linelist_niti_25') }}
),

ongoing_work_orders AS (
    SELECT
        work_order_id,
        MAX(encounter_date_time) AS max_encounter_date_time
    FROM {{ ref('daily_farmer_linelist_niti_25') }}
    GROUP BY work_order_id
),

gp_work_orders AS (
    SELECT DISTINCT
        work_order_id
    FROM {{ ref('gp_linelist_approval_niti_25') }}
)


SELECT
    w.dam,
    w.work_order_name,
    w.work_order_id,
    w.state,
    w.village,
    w.district,
    w.taluka,
    w.ngo_name,
    w.registration_date,
    c.endline_date,
    CASE
        WHEN c.work_order_id IS NOT NULL THEN 'Completed'
        WHEN o.work_order_id IS NOT NULL THEN 'Ongoing'
        ELSE 'Not Started'
    END AS project_status,
    o.max_encounter_date_time as farmer_date
FROM approved_work_orders AS w
LEFT JOIN completed_work_orders AS c
    ON w.work_order_id = c.work_order_id
LEFT JOIN ongoing_work_orders AS o
    ON w.work_order_id = o.work_order_id
WHERE NOT (
    g.work_order_id IS NOT NULL
    AND o.work_order_id IS NULL
)
