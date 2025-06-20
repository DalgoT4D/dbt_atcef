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
        cast(nullif("Total_amount_paid__NGO___Farmers_", '') as numeric) as total_amount_paid,
        cast(nullif("Amount_paid_towards_Silt_Excavation__In_lakh_", '') as numeric) as silt_excavation_paid_lakh,
        cast(nullif("Amount_paid_towards_subsidy_to_farmers__In_lakh_", '') as numeric) as subsidy_paid_lakh,
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta
    from source
)

select * from renamed
