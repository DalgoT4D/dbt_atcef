{{ config(
  materialized='table',
  tags=["final","final_niti_union", "niti"]
) }}

SELECT DISTINCT 
    machine_id,
    subject_type,
    machine_voided,
    machine_name,
    type_of_machine,
    dam,
    district,
    state,
    taluka,
    village,
    ngo_name,
    endline_status
FROM {{ ref('machine_endline_niti_2022') }}

UNION

SELECT DISTINCT 
    machine_id,
    subject_type,
    machine_voided,
    machine_name,
    type_of_machine,
    dam,
    district,
    state,
    taluka,
    village,
    ngo_name,
    endline_status
FROM {{ ref('machine_endline_niti_2023') }}

UNION

SELECT DISTINCT 
    machine_id,
    subject_type,
    machine_voided,
    machine_name,
    type_of_machine,
    dam,
    district,
    state,
    taluka,
    village,
    ngo_name,
    endline_status
FROM {{ ref('machine_endline_niti_2024') }}

UNION

SELECT DISTINCT 
    machine_id,
    subject_type,
    machine_voided,
    machine_name,
    type_of_machine,
    dam,
    district,
    state,
    taluka,
    village,
    ngo_name,
    endline_status
FROM {{ ref('machine_endline_niti_2025') }}
