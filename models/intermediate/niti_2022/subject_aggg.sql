WITH base AS (
    SELECT
        dam,
        district,
        state,
        taluka,
        SUM(CAST(total_silt_required AS NUMERIC)) AS total_silt_target, -- Cast to numeric type
        SUM(CAST(silt_to_be_excavated AS NUMERIC)) AS silt_to_be_excavated
    FROM staging.subjects_niti
    GROUP BY dam, district, state, taluka
)

SELECT
    *,
    (total_silt_target - silt_to_be_excavated) AS remaining_silt_to_excavate,
    (silt_to_be_excavated / total_silt_target) * 100 AS percentage_excavated
FROM base