{{ config(
  materialized='table',
  tags=["final","final_org"]
) }}

SELECT DISTINCT
    eid,
    farmer_id,
    work_order_id,
    state,
    village,
    machine_sub_id,
    district,
    taluka,
    dam,
    ngo_name,
    farmer_name,
    mobile_number,
    mobile_verified,
    category_of_farmer,
    work_order_name,
    silt_target,
    total_silt_carted,
    date_time,
    'Niti Aayog' AS project
FROM {{ ref('farmer_calc_silt_niti_union') }}

UNION

SELECT DISTINCT
    eid,
    farmer_id,
    work_order_id,
    state,
    village,
    machine_sub_id,
    district,
    taluka,
    dam,
    ngo_name,
    farmer_name,
    mobile_number,
    mobile_verified,
    category_of_farmer,
    work_order_name,
    silt_target,
    total_silt_carted,
    date_time,
    'Project A' AS project
FROM {{ ref('farmer_calc_silt_gramin_union') }}

UNION

SELECT DISTINCT
    eid,
    farmer_id,
    work_order_id,
    state,
    village,
    machine_sub_id,
    district,
    taluka,
    dam,
    ngo_name,
    farmer_name,
    mobile_number,
    mobile_verified,
    category_of_farmer,
    work_order_name,
    silt_target,
    total_silt_carted,
    date_time,
    'GDGS' AS project
FROM {{ ref('farmer_calc_silt_gdgs_union') }}