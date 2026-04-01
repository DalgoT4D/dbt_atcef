-- Machine efficiency metrics for GraminPA 2025 using analytics lineage only.
{{ config(
  materialized='table',
  tags=["analytics", "analytics_gramin_2025", "analytical_models", "reports_graminpa_2025", "ss_graminpa_2025"]
) }}

WITH working_hours AS (
    SELECT
        dm.machine_id,
        SUM(COALESCE(dm.working_hours, 0)) AS total_working_hours,
        MAX(CAST(dm.encounter_date_time AS DATE)) AS date_time
    FROM {{ ref('daily_machine_linelist_graminpa_25') }} AS dm
    WHERE
        dm.machine_id IS NOT NULL
        AND dm.working_hours::text != 'NaN'
    GROUP BY dm.machine_id
),

silt_carted AS (
    SELECT
        df.machine_id,
        SUM(COALESCE(df.silt_carted, 0)) AS total_silt_carted
    FROM {{ ref('daily_farmer_linelist_graminpa_25') }} AS df
    WHERE
        df.machine_id IS NOT NULL
        AND df.silt_carted::text != 'NaN'
    GROUP BY df.machine_id
),

efficiency AS (
    SELECT
        w.machine_id,
        COALESCE(s.total_silt_carted, 0) AS total_silt_carted,
        COALESCE(w.total_working_hours, 0) AS total_working_hours,
        ROUND(CAST(COALESCE(s.total_silt_carted, 0) AS NUMERIC) / NULLIF(CAST(COALESCE(w.total_working_hours, 0) AS NUMERIC), 0), 2) AS avg_silt_excavated_per_hour
    FROM working_hours AS w
    LEFT JOIN silt_carted AS s
        ON w.machine_id = s.machine_id
    WHERE
        COALESCE(s.total_silt_carted, 0) > 0
        AND COALESCE(w.total_working_hours, 0) > 0
),

machine_details AS (
    SELECT
        m.subject_id AS machine_id,
        m.machine_name,
        m.machine_type AS type_of_machine,
        m.dam,
        m.district,
        m.state,
        m.taluka,
        m.village,
        COALESCE(NULLIF(m.stakeholder_responsible, ''), 'Unknown') AS ngo_name,
        m.voided AS machine_voided,
        m.approval_status AS machine_approval_status
    FROM {{ ref('machine_regn_graminpa_25') }} AS m
    WHERE m.approval_status = 'Approved'
),

final AS (
    SELECT
        m.machine_id,
        m.machine_name,
        m.type_of_machine,
        m.dam,
        m.district,
        m.state,
        m.taluka,
        m.village,
        m.ngo_name,
        w.date_time,
        e.total_silt_carted,
        e.total_working_hours,
        e.avg_silt_excavated_per_hour,
        CASE
            WHEN LOWER(COALESCE(m.type_of_machine, '')) = 'jcb' AND e.avg_silt_excavated_per_hour < 39.2 THEN 'Below Benchmark'
            WHEN LOWER(COALESCE(m.type_of_machine, '')) = 'jcb' AND e.avg_silt_excavated_per_hour >= 39.2 THEN 'Above Benchmark'
            WHEN LOWER(COALESCE(m.type_of_machine, '')) = 'poclain' AND e.avg_silt_excavated_per_hour < 89.6 THEN 'Below Benchmark'
            WHEN LOWER(COALESCE(m.type_of_machine, '')) = 'poclain' AND e.avg_silt_excavated_per_hour >= 89.6 THEN 'Above Benchmark'
            ELSE 'Unknown'
        END AS benchmark_classification
    FROM efficiency AS e
    INNER JOIN working_hours AS w
        ON e.machine_id = w.machine_id
    INNER JOIN machine_details AS m
        ON e.machine_id = m.machine_id
    WHERE
        m.machine_voided = FALSE
        AND m.machine_approval_status = 'Approved'
)

SELECT * FROM final
