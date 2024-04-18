{{ config(
  materialized='table'
) }}


with cte as (SELECT
    "ID" AS uid, 
    observations->>'First name' AS first_name,
    location->>'Dam' AS dam,
    location->>'District' AS district,
    location->>'State' AS state,
    location->>'Taluka' AS taluka,
    location->>'GP/Village' AS village,
    observations ->> 'Type of Machine' AS type_of_machine,
    observations ->> 'Category of farmer' AS category_of_farmer,
    observations ->'Mobile Number'->>'phoneNumber' AS mobile_number,
    (observations -> 'Mobile Number'->>'verified')::boolean AS mobile_verified,
    observations ->> 'Silt to be excavated as per plan' as silt_to_be_excavated_as_per_plan,
    observations ->> 'Total silt required' as total_silt_required,
    observations ->> 'Total Silt Excavated' as total_silt_excavated,
    observations ->> 'Farmer contribution per trolley' as farmer_contribution_per_trolley,
    observations ->> 'Number of trolleys required' as number_of_trolleys_required,
    observations ->> 'Number of hywas required' as number_of_hywas_required,
    observations ->> 'Name of WB' as name_of_WB
FROM 
    {{ source('source_gdgsom_surveys', 'subjects_2024') }}
)


{{ dbt_utils.deduplicate(
    relation='cte',
    partition_by='uid',
    order_by='uid desc',
   )
}}
