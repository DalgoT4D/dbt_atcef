-- Farmer registration mart combining subject, location, and approval details for graminpa 2025.
{{ config(
  materialized='table',
  tags=["analytics","analytics_graminpa_2025", "registrations_graminpa_2025"]
) }}

with farmer_data as (
SELECT 
f.*,
l.*,
a.approval_status

FROM 
{{ ref('dim_subjects_farmer_graminpa_25') }} AS f
LEFT JOIN 
{{ ref('location_graminpa_25') }} AS l
    ON f.location_id = l.address_id
LEFT JOIN 
{{ ref('approval_status_graminpa_25') }} AS a
    ON f.subject_id = a.entity_id

WHERE f.voided != TRUE),

-- select 
-- subject_id,
-- registration_date,
-- farmer_first_name as farmer_name,
-- land_holding_acres as land_holding,
-- mobile_number,
-- mobile_verified_status,
-- farmer_category,
-- total_silt_required,
-- number_hywas_required,
-- number_trolleys_required,
-- capacity_trolleys_cum,
-- farmer_contribution_per_trolley,
-- silt_target,
-- state,
-- district,
-- taluka,
-- village,
-- dam,
-- gram_panchayat_name as gp,
-- stakeholder_responsible,
-- approval_status
-- from farmer_data


dedup AS (
    SELECT
        subject_id,
        registration_date,
        farmer_first_name AS farmer_name,
        land_holding_acres AS land_holding,
        mobile_number,
        mobile_verified_status,
        farmer_category,
        total_silt_required,
        number_hywas_required,
        number_trolleys_required,
        capacity_trolleys_cum,
        farmer_contribution_per_trolley,
        silt_target,
        state,
        district,
        taluka,
        village,
        dam,
        gram_panchayat_name AS gp,
        stakeholder_responsible,
        approval_status,
        ROW_NUMBER() OVER (
            PARTITION BY
                subject_id,
                registration_date,
                farmer_first_name,
                land_holding_acres,
                mobile_number,
                mobile_verified_status,
                farmer_category,
                total_silt_required,
                number_hywas_required,
                number_trolleys_required,
                capacity_trolleys_cum,
                farmer_contribution_per_trolley,
                silt_target,
                state,
                district,
                taluka,
                village,
                dam,
                gram_panchayat_name,
                stakeholder_responsible,
                approval_status
            ORDER BY registration_date DESC
        ) AS rn
    FROM farmer_data
)

SELECT *
FROM dedup
WHERE rn = 1

