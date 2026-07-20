-- Aggregates daily farmer encounters by dam to report silt carted splits by farmer category,
-- non-farm purpose silt, daily recording counts, and endline completion.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gramin_2026", "analytical_models", "reports_graminpa_2026", "aggregated_graminpa_26"]
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
    MAX(d.date_time) AS date_time,
    COUNT(d.*) AS total_daily_recordings,
    fc.farmer_category,
    CASE WHEN fc.mobile_verified_status = 'true' THEN 1 ELSE 0 END AS mobile_verified_status

    FROM {{ ref('farmer_regn_graminpa_26') }} AS fc
    LEFT JOIN {{ ref('daily_farmer_linelist_graminpa_26') }} AS d
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
    MAX(de.date_time) AS date_time, -- Source: latest farmer daily-recording encounter date per dam; safe to change/remove.
    SUM(de.total_daily_recordings) AS total_daily_recordings,
    SUM(de.mobile_verified_status) AS mobile_verified_farmers,
    COUNT(de.farmer_id) AS registered_farmers,
    COUNT(CASE WHEN fe.farmer_beneficiary_id IS NOT NULL THEN fe.farmer_beneficiary_id END) AS farmers_with_endline_done,
    COUNT(Case when de.farmer_category = 'Marginal: 0-2.47 acres' THEN de.farmer_id END) AS marginal_farmers,
    COUNT(Case when de.farmer_category = 'Small: 2.48 to 4.94 acres' THEN de.farmer_id END) AS small_farmers,
    COUNT(Case when de.farmer_category = 'Semi Medium: 4.95 to 9.88 acres' THEN de.farmer_id END) AS semi_medium_farmers,
    COUNT(Case when de.farmer_category = 'Medium: 9.89 acres to 24.7 acres' THEN de.farmer_id END) AS medium_farmers,
    COUNT(Case when de.farmer_category = 'Large: Above 24.7 acres' THEN de.farmer_id END) AS large_farmers,
    SUM(CASE WHEN de.farmer_category = 'Disabled' THEN 1 ELSE 0 END) 
    AS disabled_farmers, -- account for nulls
    SUM(CASE WHEN de.farmer_category = 'Widow' THEN 1 ELSE 0 END) 
    AS widow_farmers, -- account for nulls
    SUM(CASE 
        WHEN de.farmer_category = 'Family of farmer who committed suicide' 
        THEN 1 
        ELSE 0 
    END) AS family_members_suicide_farmers, -- account for nulls
    
    /* silt carted by farmers (cu.m) */
    SUM(CASE WHEN de.farmer_category = 'Marginal: 0-2.47 acres' THEN de.silt_carted ELSE 0 END) AS marginal_farmers_silt_carted,
    SUM(CASE WHEN de.farmer_category = 'Small: 2.48 to 4.94 acres' THEN de.silt_carted ELSE 0 END) AS small_farmers_silt_carted,
    SUM(CASE WHEN de.farmer_category = 'Semi Medium: 4.95 to 9.88 acres' THEN de.silt_carted ELSE 0 END) AS semi_medium_farmers_silt_carted,
    SUM(CASE WHEN de.farmer_category = 'Medium: 9.89 acres to 24.7 acres' THEN de.silt_carted ELSE 0 END) AS medium_farmers_silt_carted,
    SUM(CASE WHEN de.farmer_category = 'Large: Above 24.7 acres' THEN de.silt_carted ELSE 0 END) AS large_farmers_silt_carted,
    SUM(CASE WHEN de.farmer_category = 'Disabled' THEN COALESCE(de.silt_carted, 0) ELSE 0 END) AS disabled_farmers_silt_carted, -- account for nulls
    SUM(CASE WHEN de.farmer_category = 'Widow' THEN COALESCE(de.silt_carted, 0) ELSE 0 END) AS widow_farmers_silt_carted,-- account for nulls
    SUM(CASE WHEN de.farmer_category = 'Family of farmer who committed suicide' THEN COALESCE(de.silt_carted, 0) ELSE 0 END) AS family_members_suicide_farmers_silt_carted,
-- account for nulls

        /* silt carted for non-farm purpose (cu.m) */
    SUM(CASE WHEN de.farmer_category = 'Marginal: 0-2.47 acres' THEN de.amt_silt_used_non_farm_purpose ELSE 0 END) AS marginal_farmers_silt_for_non_farm_purpose,
    SUM(CASE WHEN de.farmer_category = 'Small: 2.48 to 4.94 acres' THEN de.amt_silt_used_non_farm_purpose ELSE 0 END) AS small_farmers_silt_for_non_farm_purpose,
    SUM(CASE WHEN de.farmer_category = 'Semi Medium: 4.95 to 9.88 acres' THEN de.amt_silt_used_non_farm_purpose ELSE 0 END) AS semi_medium_farmers_silt_for_non_farm_purpose,
    SUM(CASE WHEN de.farmer_category = 'Medium: 9.89 acres to 24.7 acres' THEN de.amt_silt_used_non_farm_purpose ELSE 0 END) AS medium_farmers_silt_for_non_farm_purpose,
    SUM(CASE WHEN de.farmer_category = 'Large: Above 24.7 acres' THEN de.amt_silt_used_non_farm_purpose ELSE 0 END) AS large_farmers_silt_for_non_farm_purpose,
  SUM(CASE WHEN de.farmer_category = 'Disabled' THEN COALESCE(de.amt_silt_used_non_farm_purpose, 0) ELSE 0 END) AS disabled_farmers_silt_for_non_farm_purpose,
SUM(CASE WHEN de.farmer_category = 'Widow' THEN COALESCE(de.amt_silt_used_non_farm_purpose, 0) ELSE 0 END) AS widow_farmers_silt_for_non_farm_purpose,
SUM(CASE WHEN de.farmer_category = 'Family of farmer who committed suicide' THEN COALESCE(de.amt_silt_used_non_farm_purpose, 0) ELSE 0 END) AS family_members_suicide_farmers_silt_for_non_farm_purpose,

    SUM(fe.area_silt_spread) AS area_silt_spread
    FROM daily_summed AS de
    LEFT JOIN {{ ref('farmer_endline_linelist_graminpa_26') }} AS fe
        ON de.farmer_id = fe.farmer_beneficiary_id

    GROUP BY
        de.dam,
        de.state,
        de.district,
        de.taluka,
        de.village,
        de.gp,
        de.stakeholder_responsible
