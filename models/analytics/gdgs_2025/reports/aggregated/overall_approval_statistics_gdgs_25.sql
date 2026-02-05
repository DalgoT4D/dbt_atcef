-- Unions the approval linelists for every entity type and counts approved, pending, and rejected records.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2025","analytical_models","reports_gdgs_2025", "aggregated_gdgs_25"]
) }}

WITH entity_statuses AS (
    SELECT 'Farmer Registration' AS entity, 
        state, district, taluka, village, dam, --stakeholder_responsible,
        approval_status
    FROM {{ ref('farmer_linelist_approval_gdgs_25') }}

    UNION ALL

    SELECT 'Farmer Endline' AS entity, 
    state, district, taluka, village, dam, --stakeholder_responsible,
        approval_status
    FROM {{ ref('farmer_endline_linelist_approval_gdgs_25') }}

    UNION ALL

    SELECT 'Machine Registration' AS entity, 
        state, district, taluka, village, dam, --stakeholder_responsible,
        approval_status
    FROM {{ ref('machine_linelist_approval_gdgs_25') }}

    UNION ALL

    SELECT 'Machine Endline' AS entity, 
        state, district, taluka, village, dam, --stakeholder_responsible,
        approval_status
    FROM {{ ref('machine_endline_linelist_approval_gdgs_25') }}

    UNION ALL

    SELECT 'Work order registration' AS entity,
        state, district, taluka, village, dam, --stakeholder_responsible,
        approval_status
    FROM {{ ref('workorder_linelist_approval_gdgs_25') }}

    UNION ALL

    SELECT
        'Work order daily recording - Farmer' AS entity,
        state, district, taluka, village, dam, --stakeholder_responsible,
        COALESCE(encounter_approval_status) AS approval_status
    FROM {{ ref('daily_farmer_linelist_approval_gdgs_25') }}

    UNION ALL

    SELECT 'Work order daily recording - Machine' AS entity, 
        state, district, taluka, village, dam, --stakeholder_responsible,
        COALESCE(encounter_approval_status) AS approval_status
FROM {{ ref('daily_machine_linelist_approval_gdgs_25') }}

    UNION ALL

    SELECT 'Work order endline' AS entity,
        state, district, taluka, village, dam, --stakeholder_responsible,
        approval_status
    FROM {{ ref('workorder_endline_linelist_approval_gdgs_25') }}
)

-- SELECT
--     entity,
--     COUNT(*) AS total_registrations,
--     COUNT(*) FILTER (
--         WHERE LOWER(COALESCE(approval_status, '')) = 'approved'
--     ) AS approved_count,
--     COUNT(*) FILTER (
--         WHERE LOWER(COALESCE(approval_status, '')) = 'pending'
--     ) AS pending_count,
--     COUNT(*) FILTER (
--         WHERE LOWER(COALESCE(approval_status, '')) = 'rejected'
--     ) AS rejected_count
-- FROM entity_statuses
-- GROUP BY entity
-- ORDER BY entity
SELECT
    entity as Characteristic,
    state,
    district,
    taluka,
    village,
    dam,
   -- stakeholder_responsible,

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
GROUP BY
    entity,
    state,
    district,
    taluka,
    village,
    dam
    --stakeholder_responsible

ORDER BY
    entity,
    state,
    district,
    taluka,
    village