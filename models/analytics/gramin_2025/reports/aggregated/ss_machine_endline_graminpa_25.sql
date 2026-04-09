-- Machine data with machine endline status for GraminPA 2025 using analytics
-- lineage only, limited to machines that appear in approved daily recordings.
{{ config(
  materialized='table',
  tags=["ss_2025", "ss_graminpa_2025"]
) }}

WITH machines_with_daily_recording AS (
    SELECT DISTINCT
        machine_id
    FROM {{ ref('daily_machine_linelist_graminpa_25') }}
    WHERE machine_id IS NOT NULL
),

approved_machines AS (
    SELECT
        m.subject_id AS machine_id,
        m.subject_type,
        m.voided AS machine_voided,
        m.machine_name,
        m.machine_type AS type_of_machine,
        m.dam,
        m.district,
        m.state,
        m.taluka,
        m.village,
        m.approval_status AS machine_approval_status,
        COALESCE(NULLIF(m.stakeholder_responsible, ''), 'Unknown') AS ngo_name
    FROM {{ ref('machine_regn_graminpa_25') }} AS m
    WHERE m.approval_status = 'Approved'
),

machine_endline_exists AS (
    SELECT DISTINCT
        machine_id
    FROM {{ ref('machine_endline_linelist_graminpa_25') }}
    WHERE machine_id IS NOT NULL
)

SELECT
    m.machine_id,
    m.subject_type,
    m.machine_voided,
    m.machine_name,
    m.type_of_machine,
    m.dam,
    m.district,
    m.state,
    m.taluka,
    m.village,
    m.machine_approval_status,
    m.ngo_name,
    CASE WHEN e.machine_id IS NOT NULL THEN 'Endline Done' ELSE 'Endline Not Done' END AS endline_status
FROM approved_machines AS m
INNER JOIN machines_with_daily_recording AS d
    ON m.machine_id = d.machine_id
LEFT JOIN machine_endline_exists AS e
    ON m.machine_id = e.machine_id
