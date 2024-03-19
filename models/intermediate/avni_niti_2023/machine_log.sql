{{ config(
  materialized='table'
) }}


select 

observations->>'Total working hours of machine by time' as total_working_hours,
observations->>'Working Hours as per time' as working_hours_as_per_time


from {{ source('source_atecf_surveys', 'encounter_2023') }}