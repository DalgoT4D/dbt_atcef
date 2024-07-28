{{ config(
  materialized='table'
) }}


with cte as (select 
date_time,
state,
district,
taluka,
village,
dam,
ngo_name,
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
ngo_name,
type_of_machine,
avg_silt_excavated_per_hour,
benchmark_classification 
from {{ref('machine_niti_metric_23')}}

union all 

select 
date_time,
state,
district,
taluka,
village,
dam,
ngo_name,
type_of_machine,
avg_silt_excavated_per_hour,
benchmark_classification 
from {{ref('machine_niti_metric_24')}})


select * from cte where avg_silt_excavated_per_hour::text != 'NaN'
