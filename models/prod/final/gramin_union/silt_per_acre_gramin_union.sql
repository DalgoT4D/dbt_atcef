{{ config(materialized='table', tags=["final", "final_gramin_union", "gramin_niti"]) }}

{% set columns = ['work_order_name', 'state', 'district', 'taluka', 'dam', 'ngo_name', 'village', 'endline_status', 'date_time', 'silt_achieved_by_endline_farmers', 'total_farm_area_silt_is_spread_on', 'silt_per_acre', 'silt_per_acre_benchmark_classification'] %}

SELECT {{ columns | join(', ') }} FROM {{ ref('silt_per_acre_gramin') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('silt_per_acre_gramin_25') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('ss_silt_per_acre_graminpa_26') }}
