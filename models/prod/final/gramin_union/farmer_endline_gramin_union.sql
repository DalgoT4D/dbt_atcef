{{ config(materialized='table', tags=["final", "final_gramin_union", "gramin_niti"]) }}

{% set columns = ['farmer_id', 'farmer_name', 'state', 'taluka', 'village', 'dam', 'district', 'ngo_name', 'type_of_land_silt_is_spread_on', 'total_farm_area_silt_is_spread_on', 'distance_from_waterbody', 'endline_status'] %}

SELECT {{ columns | join(', ') }} FROM {{ ref('farmer_endline_gramin') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('farmer_endline_gramin_25') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('ss_farmer_endline_graminpa_26') }}
