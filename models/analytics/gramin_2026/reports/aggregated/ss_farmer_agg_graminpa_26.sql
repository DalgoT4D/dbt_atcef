-- Aggregated farmer status by location for GraminPA 2026 using analytics lineage.
{{ config(
  materialized='table',
  tags=["ss_2026", "ss_graminpa_2026"]
) }}

WITH farmer_latest_activity AS (
    SELECT
        d.farmer_id,
        fr.state,
        fr.district,
        fr.village,
        fr.taluka,
        fr.dam,
        fr.stakeholder_responsible AS ngo_name,
        fr.farmer_name,
        fr.mobile_number,
        fr.mobile_verified_status,
        fr.farmer_category,
        MAX(CAST(d.encounter_date_time AS DATE)) AS date_time
    FROM {{ ref('daily_farmer_linelist_graminpa_26') }} AS d
    INNER JOIN {{ ref('farmer_regn_graminpa_26') }} AS fr
        ON d.farmer_id = fr.subject_id
    GROUP BY
        d.farmer_id,
        fr.state,
        fr.district,
        fr.village,
        fr.taluka,
        fr.dam,
        fr.stakeholder_responsible,
        fr.farmer_name,
        fr.mobile_number,
        fr.mobile_verified_status,
        fr.farmer_category
)

SELECT
    dam,
    date_time,
    state,
    district,
    taluka,
    village,
    ngo_name,
    SUM(CASE WHEN LOWER(COALESCE(mobile_verified_status, '')) = 'true' THEN 1 ELSE 0 END) AS verified_farmers,
    SUM(CASE WHEN LOWER(COALESCE(mobile_verified_status, '')) = 'false' THEN 1 ELSE 0 END) AS unverified_farmers,
    COUNT(*) AS total,
    COUNT(CASE WHEN farmer_category = 'Marginal (0-2.49 acres)' THEN 1 END) AS vulnerable_marginal,
    COUNT(CASE WHEN farmer_category = 'Small (2.5-4.99 acres)' THEN 1 END) AS vulnerable_small,
    COUNT(CASE WHEN farmer_category IN ('Semi-medium (5-9.99 acres)', 'Semi-medium (5 to 9.99 acre)', 'Semi-medium (5-9.99 acre)') THEN 1 END) AS semi_medium,
    COUNT(CASE WHEN farmer_category = 'Medium (10-24.99 acres)' THEN 1 END) AS medium,
    COUNT(CASE WHEN farmer_category IN ('Large (25+ acres)', 'Large (above 25 acres)') THEN 1 END) AS large
FROM farmer_latest_activity
GROUP BY
    dam,
    date_time,
    state,
    district,
    taluka,
    village,
    ngo_name
