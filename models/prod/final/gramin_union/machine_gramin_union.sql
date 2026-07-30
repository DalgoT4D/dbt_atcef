{{ config(materialized='table', tags=["final", "final_gramin_union", "gramin_niti"]) }}

{% set columns = ['machine_id', 'machine_name', 'type_of_machine', 'dam', 'district', 'state', 'taluka', 'village', 'ngo_name', 'total_silt_carted', 'total_working_hours', 'avg_silt_excavated_per_hour', 'benchmark_classification', 'date_time'] %}

SELECT {{ columns | join(', ') }} FROM {{ ref('machine_gramin_metric') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('machine_gramin_metric_25') }}
UNION ALL
SELECT {{ columns | join(', ') }} FROM {{ ref('ss_machine_metric_graminpa_26') }}
