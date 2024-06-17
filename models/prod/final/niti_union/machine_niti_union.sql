{{ config(
  materialized='table'
) }}


select 
date_time,
state,
district,
taluka,
village,
dam,
type_of_machine,
avg_silt_excavated_per_hour,
benchmark_classification 
from {{ref('machine_niti_metric_22')}}

union all 

select 
date_time,
state,
district,
taluka,
village,
dam,
type_of_machine,
avg_silt_excavated_per_hour,
benchmark_classification 
from {{ref('machine_niti_metric_23')}}
