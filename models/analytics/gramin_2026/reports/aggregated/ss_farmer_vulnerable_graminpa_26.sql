-- Farmer vulnerability split for GraminPA 2026 using distinct farmers from
-- the approved daily farmer linelist and registration categories.
{{ config(
  materialized='table',
  tags=["ss_2026", "ss_graminpa_2026"]
) }}

WITH farmer_activity AS (
    SELECT
        farmer_id,
        SUM(CASE WHEN silt_carted::text != 'NaN' THEN COALESCE(silt_carted, 0) ELSE 0 END) AS total_silt_carted
    FROM {{ ref('daily_farmer_linelist_graminpa_26') }}
    WHERE farmer_id IS NOT NULL
    GROUP BY farmer_id
),

farmer_counts AS (
    SELECT
        fr.state,
        fr.district,
        fr.taluka,
        fr.village,
        fr.dam,
        fr.stakeholder_responsible AS ngo_name,
        COUNT(CASE WHEN fr.farmer_category IN ('Marginal: 0-2.47 acres', 'Small: 2.48 to 4.94 acres') THEN fr.subject_id END) AS vulnerable_farmers,
        COUNT(CASE WHEN fr.farmer_category IN ('Semi Medium: 4.95 to 9.88 acres', 'Medium: 9.89 acres to 24.7 acres', 'Large: Above 24.7 acres') THEN fr.subject_id END) AS other_farmers,
        SUM(CASE WHEN fr.farmer_category IN ('Marginal: 0-2.47 acres', 'Small: 2.48 to 4.94 acres') THEN COALESCE(d.total_silt_carted, 0) ELSE 0 END) AS vulnerable_silt,
        SUM(CASE WHEN fr.farmer_category IN ('Semi Medium: 4.95 to 9.88 acres', 'Medium: 9.89 acres to 24.7 acres', 'Large: Above 24.7 acres') THEN COALESCE(d.total_silt_carted, 0) ELSE 0 END) AS other_silt
    FROM farmer_activity AS d
    INNER JOIN {{ ref('farmer_regn_graminpa_26') }} AS fr
        ON d.farmer_id = fr.subject_id
    GROUP BY
        fr.state,
        fr.district,
        fr.taluka,
        fr.village,
        fr.dam,
        fr.stakeholder_responsible
)

SELECT
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name,
    'vulnerable' AS farmer_type,
    COALESCE(vulnerable_farmers, 0) AS farmers_count,
    COALESCE(vulnerable_silt, 0) AS total_silt_carted
FROM farmer_counts
WHERE COALESCE(vulnerable_farmers, 0) > 0

UNION ALL

SELECT
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name,
    'others' AS farmer_type,
    COALESCE(other_farmers, 0) AS farmers_count,
    COALESCE(other_silt, 0) AS total_silt_carted
FROM farmer_counts
WHERE COALESCE(other_farmers, 0) > 0
