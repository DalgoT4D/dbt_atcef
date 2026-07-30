{{ config(materialized='table', tags=["final", "final_gramin_union", "gramin_niti"]) }}

{% set columns = ['dam', 'work_order_id', 'state', 'village', 'district', 'taluka', 'endline_date', 'farmer_date', 'ngo_name', 'project_status', 'work_order_endline_status'] %}

SELECT {{ columns | join(', ') }} FROM {{ ref('progress_waterbodies_gramin') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('progress_waterbodies_gramin_25') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('ss_progress_dam_graminpa_26') }}
