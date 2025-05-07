{{ config(
  materialized='table',
  tags=["final","final_gdgs_union", "gdgs"]
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
FROM {{ ref('machine_endline') }}

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
FROM {{ ref('machine_endline_gdgs_2023') }}

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
FROM {{ ref('machine_endline_gdgs_25') }}
