{{ config(
  materialized='table',
  tags=["final", "final_gramin_union", "gramin_niti"]
) }}

select
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
    machine_approval_status,
    ngo_name,
    endline_status
from {{ ref('machine_endline_gramin') }}

union all

select
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
    machine_approval_status,
    ngo_name,
    endline_status
from {{ ref('machine_endline_gramin_25') }}

union all

select
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
    machine_approval_status,
    ngo_name,
    endline_status
from {{ ref('machine_endline_gramin_26') }}
