{{ config(
    materialized='table',
    tags=["final","final_gdgs_2025", "gdgs_2025", "gdgs"]
) }}

WITH farmer_endline_deduped AS (
    SELECT DISTINCT ON (farmer_id)
        farmer_id,
        total_farm_area_silt_is_spread_on
    FROM {{ ref('farmer_endline_gdgs_25') }}
    ORDER BY farmer_id DESC
),

base AS (
    SELECT
        f.farmer_id,
        f.dam,
        f.silt_target,
        f.total_silt_carted,
        f.category_of_farmer,
        f.district AS farmer_district,
        e.total_farm_area_silt_is_spread_on
    FROM {{ ref('farmer_calc_silt_gdgs_25') }} f
    LEFT JOIN farmer_endline_deduped e
        ON f.farmer_id = e.farmer_id
    WHERE f.total_silt_carted IS NOT NULL AND f.total_silt_carted > 0
),

sheet_data AS (
    SELECT
        district,
        total_amount_paid,
        silt_excavation_paid_lakh,
        subsidy_paid_lakh
    FROM {{ ref('mh_dashboard_int') }}
),

aggregated AS (
    SELECT DISTINCT
        ROW_NUMBER() OVER () AS id,
        b.farmer_district AS district,
        COUNT(DISTINCT dam) AS no_of_waterbodies,
        SUM(DISTINCT silt_target) AS silt_target_cum,
        SUM(total_silt_carted) AS total_silt_excavated_cum,
        ROUND(SUM(total_silt_carted) / NULLIF(SUM(DISTINCT silt_target), 0) * 100, 2) AS silt_achievement_percent,
        ROUND(SUM(total_silt_carted) * 36.89, 2) AS expected_expenditure_lakh,
        COUNT(DISTINCT farmer_id) AS total_no_of_farmers,
        COUNT(DISTINCT CASE
            WHEN category_of_farmer IN (
                'Marginal (0-2.49 acres)', 'Small (2.5-4.99 acres)',
                'Widow', 'Disabled', 'Family of farmer who committed suicide'
            ) THEN farmer_id
        END) AS no_of_farmers_eligible_for_subsidy,
        SUM(CASE
            WHEN category_of_farmer IN (
                'Marginal (0-2.49 acres)', 'Small (2.5-4.99 acres)',
                'Widow', 'Disabled', 'Family of farmer who committed suicide'
            ) THEN total_silt_carted
            ELSE 0
        END) AS silt_carted_by_vulnerable_farmers,
        ROUND(SUM(CASE
            WHEN category_of_farmer IN (
                'Marginal (0-2.49 acres)', 'Small (2.5-4.99 acres)',
                'Widow', 'Disabled', 'Family of farmer who committed suicide'
            ) THEN total_silt_carted
            ELSE 0
        END) * 35.75, 2) AS expected_subsidy_payout_lakh,
        sd.silt_excavation_paid_lakh,
        sd.subsidy_paid_lakh,
        sd.total_amount_paid AS total_amount_paid_till_date_lakh,
        ROUND(SUM(total_silt_carted) * 36.89 +
              SUM(CASE
                  WHEN category_of_farmer IN (
                      'Marginal (0-2.49 acres)', 'Small (2.5-4.99 acres)',
                      'Widow', 'Disabled', 'Family of farmer who committed suicide'
                  ) THEN total_silt_carted
                  ELSE 0
              END) * 35.75, 2) AS total_amount_expected_lakh,
        SUM(total_farm_area_silt_is_spread_on) AS total_area_silt_spread_acre
    FROM base b
    LEFT JOIN sheet_data sd ON b.farmer_district = sd.district
    GROUP BY b.farmer_district, sd.silt_excavation_paid_lakh, sd.subsidy_paid_lakh, sd.total_amount_paid
)

SELECT * FROM aggregated
