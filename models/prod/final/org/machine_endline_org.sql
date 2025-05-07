{{ config(
  materialized='table',
  tags=["final","final_org"]
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
    endline_status,
    'Niti Aayog' AS project
FROM {{ ref('machine_endline_niti_union') }}

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
    endline_status,
    'Project A' AS project
FROM {{ ref('machine_endline_gramin') }}

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
    endline_status,
    'GDGS' AS project
FROM {{ ref('machine_endline_gdgs_union') }}
