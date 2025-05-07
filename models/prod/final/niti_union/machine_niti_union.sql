{{ config(
  materialized='table',
  tags=["final","final_niti_union", "niti"]
) }}


with cte as (
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
        ngo_name,
        dam,
        type_of_machine,
        avg_silt_excavated_per_hour,
        benchmark_classification
    from {{ ref('machine_niti_metric_22') }}

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
        ngo_name,
        dam,
        type_of_machine,
        avg_silt_excavated_per_hour,
        benchmark_classification
    from {{ ref('machine_niti_metric_23') }}

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
        ngo_name,
        dam,
        type_of_machine,
        avg_silt_excavated_per_hour,
        benchmark_classification
    from {{ ref('machine_niti_metric_24') }}

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
        ngo_name,
        dam,
        type_of_machine,
        avg_silt_excavated_per_hour,
        benchmark_classification
    from {{ ref('machine_niti_metric_25') }}
)

select * from cte
where avg_silt_excavated_per_hour::text != 'NaN'
