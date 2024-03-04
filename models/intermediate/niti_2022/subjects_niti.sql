SELECT
    "ID",
    -- Assuming 'observations' and 'location' are JSON fields, extracting text directly.
    -- Added trimming to remove any leading/trailing spaces.
    TRIM(observations->>'First name') AS first_name_value,
    TRIM(observations->>'Last name') AS last_name_value,
    TRIM(observations->>'Date of birth') AS date_of_birth,
    -- Converting 'Year' to a numeric value; using NULLIF and CAST to handle non-numeric or empty strings gracefully.
    CAST(NULLIF(TRIM(observations->>'Year'), '') AS INTEGER) AS year_value,
    TRIM(observations->>'Gender') AS gender_value,
    TRIM(location->>'Dam') as dam,
    TRIM(location->>'District') as district,
    TRIM(location->>'State') as state,
    TRIM(location->>'Taluka') as taluka,
    TRIM(audit->>'Created at') as created_at,
    TRIM(audit->>'Last modified at') as last_modified_at,
    CAST(NULLIF(TRIM(observations->>'Capacity of trolleys in cu.m.'), '') AS NUMERIC) AS capacity_of_trolleys,
    CAST(NULLIF(TRIM(observations->>'Number of hywas required'), '') AS NUMERIC) as number_of_hywas_required,
    CAST(NULLIF(TRIM(observations->>'Number of trolleys required'), '') AS NUMERIC) as number_of_trolleys_required,
    CAST(NULLIF(TRIM(observations->>'Total silt required'), '') AS NUMERIC) as total_silt_required,
    CAST(NULLIF(TRIM(observations->>'Silt to be excavated as per plan'), '') AS NUMERIC) as silt_to_be_excavated,
    TRIM(observations->>'Amount for diesel is paid by') as amount_for_diesel_is_paid_by,
    TRIM(observations->>'Machine') AS machine_value,
    TRIM(observations->>'How the rent of the machine is paid') AS how_the_rent_of_the_machine_is_paid,
    TRIM(observations->>'Make of machine') AS make_of_machine,
    CASE 
        WHEN CAST(NULLIF(TRIM(observations->>'Manufacturing year of machine'), '') AS INTEGER) BETWEEN 1900 AND EXTRACT(YEAR FROM CURRENT_DATE)
        THEN CAST(NULLIF(TRIM(observations->>'Manufacturing year of machine'), '') AS INTEGER)
        ELSE NULL
    END AS manufacturing_year_of_machine,
    TRIM(observations->>'Model of machine') AS model_of_machine,
    CAST(NULLIF(TRIM(observations->>'Rent for machine per hour'), '') AS NUMERIC) AS rent_for_machine_per_hour,
    TRIM(observations->>'Type of Machine') AS type_of_machine

FROM rwb_niti_2022.subjects
