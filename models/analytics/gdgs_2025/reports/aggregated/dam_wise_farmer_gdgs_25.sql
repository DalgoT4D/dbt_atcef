-- Aggregates daily farmer encounters by dam to report silt carted splits by farmer category,
-- non-farm purpose silt, daily recording counts, and endline completion.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2025", "analytical_models", "reports_gdgs_2025", "aggregated_gdgs_25"]
) }}



WITH daily_summed AS (
    SELECT
    fc.farmer_name,
    fc.subject_id as farmer_id,
    fc.state,
    fc.district,
    fc.taluka,
    fc.village, 
    fc.dam,
    fc.gp,
    fc.stakeholder_responsible,
    SUM(d.silt_carted) AS silt_carted,
    SUM(d.amt_silt_used_non_farm_purpose) AS amt_silt_used_non_farm_purpose,
    COUNT(d.*) AS total_daily_recordings,
    fc.farmer_category,
    CASE WHEN fc.mobile_verified_status = 'true' THEN 1 ELSE 0 END AS mobile_verified_status

    FROM {{ ref('farmer_regn_gdgs_25') }} AS fc
    LEFT JOIN {{ ref('daily_farmer_linelist_gdgs_25') }} AS d
        ON d.farmer_id = fc.subject_id
    WHERE fc.approval_status = 'Approved'

    GROUP BY
    fc.farmer_name,
    fc.subject_id,
    fc.state,
    fc.district,
    fc.taluka, 
    fc.village,
    fc.dam,
    fc.gp,
    fc.stakeholder_responsible,
    fc.farmer_category,
    fc.mobile_verified_status
)


SELECT
    de.dam,
    de.state,
    de.district,
    de.taluka,
    de.village,
    de.gp,
    de.stakeholder_responsible,
    SUM(de.total_daily_recordings) AS total_daily_recordings,
    SUM(de.mobile_verified_status) AS mobile_verified_farmers,
    COUNT(de.farmer_id) AS registered_farmers,
    COUNT(CASE WHEN fe.farmer_beneficiary_id IS NOT NULL THEN fe.farmer_beneficiary_id END) AS farmers_with_endline_done,
    COUNT(Case when de.farmer_category = 'Marginal (0-2.49 acres)' THEN de.farmer_id END) AS marginal_farmers,
    COUNT(Case when de.farmer_category = 'Small (2.5-4.99 acres)' THEN de.farmer_id END) AS small_farmers,
    COUNT(Case when de.farmer_category = 'Semi-medium (5 to 9.99 acre)' THEN de.farmer_id END) AS semi_medium_farmers,
    COUNT(Case when de.farmer_category = 'Medium (10-24.99 acres)' THEN de.farmer_id END) AS medium_farmers,
    COUNT(Case when de.farmer_category = 'Large (above 25 acres)' THEN de.farmer_id END) AS large_farmers,
    COUNT(Case when de.farmer_category = 'Disabled' THEN de.farmer_id END) AS disabled_farmers,
    COUNT(Case when de.farmer_category = 'Widow' THEN de.farmer_id END) AS widow_farmers,
    COUNT(Case when de.farmer_category = 'Family of farmer who committed suicide' THEN de.farmer_id END) AS family_members_suicide_farmers,

    /* silt carted by farmers (cu.m) */
    SUM(CASE WHEN de.farmer_category = 'Marginal (0-2.49 acres)' THEN de.silt_carted ELSE 0 END) AS marginal_farmers_silt_carted,
    SUM(CASE WHEN de.farmer_category = 'Small (2.5-4.99 acres)' THEN de.silt_carted ELSE 0 END) AS small_farmers_silt_carted,
    SUM(CASE WHEN de.farmer_category = 'Semi-medium (5 to 9.99 acre)' THEN de.silt_carted ELSE 0 END) AS semi_medium_farmers_silt_carted,
    SUM(CASE WHEN de.farmer_category = 'Medium (10-24.99 acres)' THEN de.silt_carted ELSE 0 END) AS medium_farmers_silt_carted,
    SUM(CASE WHEN de.farmer_category = 'Large (above 25 acres)' THEN de.silt_carted ELSE 0 END) AS large_farmers_silt_carted,
    SUM(CASE WHEN de.farmer_category = 'Disabled' THEN de.silt_carted ELSE 0 END) AS disabled_farmers_silt_carted,
    SUM(CASE WHEN de.farmer_category = 'Widow' THEN de.silt_carted ELSE 0 END) AS widow_farmers_silt_carted,
    SUM(CASE WHEN de.farmer_category = 'Family of farmer who committed suicide' THEN de.silt_carted ELSE 0 END) AS family_members_suicide_farmers_silt_carted,

        /* silt carted for non-farm purpose (cu.m) */
        SUM(CASE WHEN de.farmer_category = 'Marginal (0-2.49 acres)' THEN de.amt_silt_used_non_farm_purpose ELSE 0 END) AS marginal_farmers_silt_for_non_farm_purpose,
        SUM(CASE WHEN de.farmer_category = 'Small (2.5-4.99 acres)' THEN de.amt_silt_used_non_farm_purpose ELSE 0 END) AS small_farmers_silt_for_non_farm_purpose,
        SUM(CASE WHEN de.farmer_category = 'Semi-medium (5 to 9.99 acre)' THEN de.amt_silt_used_non_farm_purpose ELSE 0 END) AS semi_medium_farmers_silt_for_non_farm_purpose,
        SUM(CASE WHEN de.farmer_category = 'Medium (10-24.99 acres)' THEN de.amt_silt_used_non_farm_purpose ELSE 0 END) AS medium_farmers_silt_for_non_farm_purpose,
        SUM(CASE WHEN de.farmer_category = 'Large (above 25 acres)' THEN de.amt_silt_used_non_farm_purpose ELSE 0 END) AS large_farmers_silt_for_non_farm_purpose,
        SUM(CASE WHEN de.farmer_category = 'Disabled' THEN de.amt_silt_used_non_farm_purpose ELSE 0 END) AS disabled_farmers_silt_for_non_farm_purpose,
        SUM(CASE WHEN de.farmer_category = 'Widow' THEN de.amt_silt_used_non_farm_purpose ELSE 0 END) AS widow_farmers_silt_for_non_farm_purpose,
        SUM(CASE WHEN de.farmer_category = 'Family of farmer who committed suicide' THEN de.amt_silt_used_non_farm_purpose ELSE 0 END) AS family_members_suicide_farmers_silt_for_non_farm_purpose,

    SUM(fe.area_silt_spread) AS area_silt_spread
    FROM daily_summed AS de
    LEFT JOIN {{ ref('farmer_endline_linelist_gdgs_25') }} AS fe
        ON de.farmer_id = fe.farmer_beneficiary_id

    GROUP BY
        de.dam,
        de.state,
        de.district,
        de.taluka,
        de.village,
        de.gp,
        de.stakeholder_responsible
