{{ config(materialized='table', tags=["final", "final_gramin_union", "gramin_niti"]) }}

{% set columns = ['work_order_name', 'date_time', 'state', 'district', 'taluka', 'dam', 'village', 'ngo_name', 'endline_status', 'type_of_land_silt_is_spread_on', 'silt_achieved_by_endline_farmers', 'total_farm_area_silt_is_spread_on'] %}

SELECT {{ columns | join(', ') }} FROM {{ ref('barren_fertile_silt_gramin') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('barren_fertile_silt_gramin_25') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('ss_barren_fertile_silt_graminpa_26') }}
