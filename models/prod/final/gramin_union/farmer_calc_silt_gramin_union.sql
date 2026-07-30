{{ config(materialized='table', tags=["final", "final_gramin_union", "gramin_niti"]) }}

{% set columns = ['eid', 'farmer_id', 'work_order_id', 'machine_sub_id', 'state', 'village', 'district', 'taluka', 'dam', 'ngo_name', 'farmer_name', 'mobile_number', 'mobile_verified', 'category_of_farmer', 'work_order_name', 'silt_target', 'total_silt_carted', 'date_time'] %}

SELECT {{ columns | join(', ') }} FROM {{ ref('farmer_calc_silt_gramin') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('farmer_calc_silt_gramin_25') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('ss_farmer_calc_silt_graminpa_26') }}
