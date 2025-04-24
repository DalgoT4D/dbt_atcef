{{ config(
  materialized='table',
  tags=["aggregated","aggregated_gramin_niti", "gramin_niti", "gramin_25"]
) }}

WITH waterbodies AS (
    SELECT
        w.dam,
        w.work_order_id,
        w.state,
        w.village,
        w.district,
        w.taluka,
        w.ngo_name,
        MAX(
            CASE
                WHEN e.encounter_type = 'Work order endline' THEN e.date_time
            END
        ) AS endline_date,
        MAX(
            CASE
                WHEN
                    e.encounter_type = 'Work order daily Recording - Farmer'
                    THEN e.date_time
            END
        ) AS farmer_date
    FROM {{ ref('work_order_gramin_25') }} AS w
    LEFT JOIN {{ ref('encounters_gramin_25') }} AS e
        ON w.work_order_id = e.subject_id
    WHERE (
        e.encounter_type IN (
            'Work order daily Recording - Farmer', 'Work order endline'
        )
        OR e.encounter_type IS NULL
    )  -- Ensure work orders with no encounters are included
    AND w.work_order_voided != TRUE
    GROUP BY
        w.dam,
        w.work_order_id,
        w.state,
        w.village,
        w.district,
        w.taluka,
        w.ngo_name,
        e.subject_id
)


SELECT
    dam,
    work_order_id,
    state,
    village,
    district,
    taluka,
    endline_date,
    farmer_date,
    ngo_name,
    CASE
        WHEN endline_date IS NOT NULL THEN 'Completed'
        WHEN farmer_date IS NOT NULL THEN 'Ongoing'
        ELSE 'Not Started'
    END AS project_status,
    CASE
        WHEN endline_date IS NOT NULL THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS work_order_endline_status
FROM waterbodies
