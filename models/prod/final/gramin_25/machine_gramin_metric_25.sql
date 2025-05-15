{{ config(
  materialized='table',
  tags=["final","final_gramin_niti", "gramin_niti", "gramin_25"]
) }}

WITH working_hours AS (
    -- Extract total working hours for each machine from "Work order daily Recording - Machine"
    SELECT
        e.machine_sub_id AS machine_id,
        SUM(e.total_working_hours) AS total_working_hours
    FROM {{ ref('machine_gramin_aggregated_25') }} AS e
    GROUP BY e.machine_sub_id
),

silt_carted AS (
    -- Extract total silt carted for each machine from "Work order daily Recording - Farmer"
    SELECT
        e.machine_sub_id AS machine_id,
        SUM(e.total_silt_carted) AS total_silt_carted
    FROM {{ ref('farmer_calc_silt_gramin_25') }} AS e
    GROUP BY e.machine_sub_id
),

efficiency AS (
    -- Calculate machine efficiency
    SELECT
        w.machine_id,
        COALESCE(s.total_silt_carted, 0) AS total_silt_carted,
        COALESCE(w.total_working_hours, 0) AS total_working_hours,
        ROUND(
            CAST(COALESCE(s.total_silt_carted, 0) AS NUMERIC)
            / NULLIF(CAST(COALESCE(w.total_working_hours, 0) AS NUMERIC), 0),
            2
        ) AS avg_silt_excavated_per_hour
    FROM working_hours AS w
    LEFT JOIN silt_carted AS s ON w.machine_id = s.machine_id
    WHERE s.total_silt_carted > 0 AND w.total_working_hours > 0
),

benchmark AS (
    -- Classify machines based on efficiency benchmarks
    SELECT
        e.machine_id,
        e.total_silt_carted,
        e.total_working_hours,
        e.avg_silt_excavated_per_hour,
        CASE
            WHEN
                m.type_of_machine = 'JCB'
                AND e.avg_silt_excavated_per_hour < 39.2
                THEN 'Below Benchmark'
            WHEN
                m.type_of_machine = 'JCB'
                AND e.avg_silt_excavated_per_hour >= 39.2
                THEN 'Above Benchmark'
            WHEN
                m.type_of_machine = 'Poclain'
                AND e.avg_silt_excavated_per_hour < 89.6
                THEN 'Below Benchmark'
            WHEN
                m.type_of_machine = 'Poclain'
                AND e.avg_silt_excavated_per_hour >= 89.6
                THEN 'Above Benchmark'
            ELSE 'Unknown'
        END AS benchmark_classification
    FROM efficiency AS e
    LEFT JOIN {{ ref('machine_gramin_aggregated_25') }} AS m
        ON e.machine_id = m.machine_sub_id
),

final AS (
    -- Join with machine details, filtering out voided/unapproved machines
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
        b.total_silt_carted,
        b.total_working_hours,
        b.avg_silt_excavated_per_hour,
        b.benchmark_classification,
        CAST(m.date_time AS DATE) AS date_time
    FROM benchmark AS b
    INNER JOIN {{ ref('machine_gramin_aggregated_25') }} AS m
        ON b.machine_id = m.machine_sub_id
    WHERE
        m.machine_voided = FALSE
        AND m.machine_approval_status = 'Approved'
)

SELECT * FROM final
