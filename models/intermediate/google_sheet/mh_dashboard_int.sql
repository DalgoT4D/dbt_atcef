{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gdgs_2025", "gdgs_2025", "gdgs"]
) }}


with source as (
    select * from {{ source('source_google_sheet', 'google_sheet_mh_stg') }} 
),

renamed as (
    select
        cast(nullif("No", '') as integer) as row_number,
        "District" as district,
CAST(NULLIF(REGEXP_REPLACE("Total_amount_paid__NGO___Farmers_", '[-,₹\s]', '', 'g'), '') AS NUMERIC) 
AS total_amount_paid,

CAST(NULLIF(REGEXP_REPLACE("Amount_paid_towards_Silt_Excavation__In_lakh_", '[-,₹\s]', '', 'g'), '') AS NUMERIC) 
AS silt_excavation_paid_lakh,

CAST(NULLIF(REGEXP_REPLACE("Amount_paid_towards_subsidy_to_farmers__In_lakh_", '[-,₹\s]', '', 'g'), '') AS NUMERIC)
AS subsidy_paid_lakh,
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta
    from source
)

select * from renamed
