-- Reuses the machine approval linelist and keeps only approved registrations.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gramin_2025","analytical_models", "reports_graminpa_2025", "cleaned_graminpa_25"]
) }}

SELECT *
FROM {{ ref('machine_linelist_approval_graminpa_25') }}
WHERE approval_status = 'Approved'
