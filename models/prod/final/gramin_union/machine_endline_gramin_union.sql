{{ config(materialized='table', tags=["final", "final_gramin_union", "gramin_niti"]) }}

{% set columns = ['machine_id', 'subject_type', 'machine_voided', 'machine_name', 'type_of_machine', 'dam', 'district', 'state', 'taluka', 'village', 'machine_approval_status', 'ngo_name', 'endline_status'] %}

SELECT {{ columns | join(', ') }} FROM {{ ref('machine_endline_gramin') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('machine_endline_gramin_25') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('ss_machine_endline_graminpa_26') }}
