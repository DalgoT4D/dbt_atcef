{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025","niti_2025","niti","analytical_models","reports_niti_2025"]
) }}

WITH entity_statuses AS (
    SELECT 'Farmer Registration' AS entity, approval_status
    FROM {{ ref('farmer_linelist_approval_niti_25') }}

    UNION ALL

    SELECT 'Farmer Endline' AS entity, approval_status
    FROM {{ ref('farmer_endline_linelist_approval_niti_25') }}

    UNION ALL

    SELECT 'Machine Registration' AS entity, approval_status
    FROM {{ ref('machine_linelist_approval_niti_25') }}

    UNION ALL

    SELECT 'Machine Endline' AS entity, approval_status
    FROM {{ ref('machine_endline_linelist_approval_niti_25') }}

    UNION ALL

    SELECT 'Work order registration' AS entity, approval_status
    FROM {{ ref('workorder_linelist_approval_niti_25') }}

    UNION ALL

    SELECT
        'Work order daily recording - Farmer' AS entity,
        COALESCE(encounter_approval_status) AS approval_status
    FROM {{ ref('daily_farmer_linelist_approval_niti_25') }}

    UNION ALL

    SELECT 'Work order daily recording - Machine' AS entity, encounter_approval_status AS approval_status
    FROM {{ ref('daily_machine_linelist_approval_niti_25') }}

    UNION ALL

    SELECT 'Work order endline' AS entity, approval_status
    FROM {{ ref('workorder_endline_linelist_approval_niti_25') }}
)

SELECT
    entity,
    COUNT(*) AS total_registrations,
    COUNT(*) FILTER (
        WHERE LOWER(COALESCE(approval_status, '')) = 'approved'
    ) AS approved_count,
    COUNT(*) FILTER (
        WHERE LOWER(COALESCE(approval_status, '')) = 'pending'
    ) AS pending_count,
    COUNT(*) FILTER (
        WHERE LOWER(COALESCE(approval_status, '')) = 'rejected'
    ) AS rejected_count
FROM entity_statuses
GROUP BY entity
ORDER BY entity
