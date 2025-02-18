{{ config(
  materialized='table',
  tags=["final","final_org"]
) }}

SELECT DISTINCT 
    machine_id,
    machine_name,
    CAST(date_time AS DATE) AS date_time,  -- Explicitly cast to DATE
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
    benchmark_classification,
    'Niti Aayog' AS project
FROM {{ ref('machine_niti_union') }}

UNION ALL  -- Use UNION ALL for better performance

SELECT DISTINCT 
    machine_id,
    machine_name,
    CAST(date_time AS DATE) AS date_time,  -- Explicitly cast to DATE
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
    benchmark_classification,
    'Project A' AS project
FROM {{ ref('machine_gramin_metric') }}

UNION ALL

SELECT DISTINCT 
    machine_id,
    machine_name,
    CAST(date_time AS DATE) AS date_time,  -- Explicitly cast to DATE
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
    benchmark_classification,
    'GDGS' AS project
FROM {{ ref('machine_gdgs_union') }}