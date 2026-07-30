{{ config(materialized='table', tags=["final", "final_gramin_union", "gramin_niti"]) }}

{% set columns = ['state', 'district', 'taluka', 'village', 'dam', 'ngo_name', 'farmer_type', 'farmers_count'] %}

SELECT {{ columns | join(', ') }} FROM {{ ref('farmer_gramin_percentage') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('farmer_gramin_percentage_25') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('ss_farmer_percentage_graminpa_26') }}
