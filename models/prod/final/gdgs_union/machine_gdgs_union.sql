{{ config(
  materialized='table',
  tags=["final","final_gdgs_union", "gdgs"]
) }}


select
    machine_id,
    machine_name,
    date_time::date,
    total_silt_carted,
    total_working_hours,
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name,
    type_of_machine,
    avg_silt_excavated_per_hour,
    benchmark_classification
from {{ ref('machine_gdgs_metric_24') }}

union all

select
    machine_id,
    machine_name,
    date_time::date,
    total_silt_carted,
    total_working_hours,
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name,
    type_of_machine,
    avg_silt_excavated_per_hour,
    benchmark_classification
from {{ ref('machine_gdgs_metric_23') }}

union all

select
    machine_id,
    machine_name,
    date_time::date,
    total_silt_carted,
    total_working_hours,
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name,
    type_of_machine,
    avg_silt_excavated_per_hour,
    benchmark_classification
from {{ ref('machine_gdgs_metric_25') }}
