{{ config(materialized='table', tags=["final", "final_gramin_union", "gramin_niti"]) }}

{% set columns = ['state', 'district', 'taluka', 'village', 'dam', 'ngo_name', 'farmer_type', 'total_silt_carted'] %}

SELECT {{ columns | join(', ') }} FROM {{ ref('farmer_silt_vulnerable_gramin') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('farmer_silt_vulnerable_gramin_25') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('ss_farmer_vulnerable_graminpa_26') }}
