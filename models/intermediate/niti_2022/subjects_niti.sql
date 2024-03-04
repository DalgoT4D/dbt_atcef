SELECT
    "ID", -- Replace 'id' with the actual unique identifier column name if different
    observations->>'First name' AS first_name_value,
    observations->>'Date of birth' AS date_of_birth,
    observations->>'Year' AS year_value,
    observations->>'Gender' AS gender_value,
    location->>'Dam' as dam,
    location->>'District' as district,
    location->>'State' as state,
    location->>'Taluka' as Taluka,
    observations->>'Capacity of trolleys in cu.m.' AS capacity_of_trolleys,
    observations->>'Number of hywas required' as number_of_hywas_required,
    observations->>'Number of trolleys required' as number_of_trolleys_required,
    observations->>'Total silt required' as total_silt_required,
    observations->>'Silt to be excavated as per plan' as silt_to_be_excavated,
    observations->>'Amount for diesel is paid by' as amount_for_diesel_is_paid_by,
    observations->>'Machine' AS machine_value,
    observations->>'How the rent of the machine is paid' AS how_the_rent_of_the_machine_is_paid,
    observations->>'Make of machine' AS make_of_machine,
    observations->>'Manufacturing year of machine' AS manufacturing_year_of_machine,
    observations->>'Model of machine' AS model_of_machine,
    observations->>'Rent for machine per hour' AS rent_for_machine_per_hour,
    observations->>'Type of Machine' AS type_of_machine

FROM rwb_niti_2022.subjects 