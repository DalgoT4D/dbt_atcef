{{ config(
  materialized='table'
) }}


select * from prod.subjects_water_bodies as a
left join prod.subject_water_encounter as b
on a.uid = b.aid