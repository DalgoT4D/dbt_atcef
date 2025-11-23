{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti"]
) }}

WITH work_metrics AS (
    SELECT
        p.work_order_id,
        w.first_name as work_order_name,
        p.state,
        p.district,
        p.village,
        p.taluka,
        p.dam,
        p.ngo_name,
        w.silt_target,
        SUM(w.working_hours_as_per_time::NUMERIC) AS machine_working_hours, -- cross checked based on source calc
        SUM(w.total_silt_excavated_encounter::NUMERIC) AS total_silt_excavated,
        SUM(w.total_silt_carted::NUMERIC) AS total_silt_carted,
        SUM(w.total_silt_excavated_by_gp_for_non_farm_purpose::NUMERIC) AS total_silt_carted_by_gp_for_non_farm_purpose,
        COUNT(DISTINCT w.farmer_sub_id) AS number_of_farmers_carting
    FROM {{ ref('progress_waterbodies_niti_2025') }} AS p
    LEFT JOIN {{ ref('work_order_2025_niti') }} AS w
        ON p.work_order_id = w.uid
    GROUP BY
        p.work_order_id,
        w.first_name,
        p.state,
        p.district,
        p.village,
        p.taluka,
        p.dam,
        p.ngo_name,
        w.silt_target
),

machine_counts AS (
    SELECT
        w.uid as work_order_id,
        COUNT(
            DISTINCT CASE
                WHEN LOWER(COALESCE(m.type_of_machine, '')) LIKE 'jcb%'
                    THEN w.machine_sub_id
            END
        ) AS jcb_machine_count,
        COUNT(
            DISTINCT CASE
                WHEN LOWER(COALESCE(m.type_of_machine, '')) LIKE 'poclain%'
                    THEN w.machine_sub_id
            END
        ) AS poclain_machine_count
    FROM {{ ref('work_order_2025_niti') }} AS w
    LEFT JOIN {{ ref('machine_niti_2025_agg') }} AS m
        ON w.machine_sub_id = m.machine_sub_id
    GROUP BY w.uid
)

SELECT
    wm.*,
    mc.jcb_machine_count,
    mc.poclain_machine_count
FROM work_metrics AS wm
LEFT JOIN machine_counts AS mc
    ON wm.work_order_id = mc.work_order_id
