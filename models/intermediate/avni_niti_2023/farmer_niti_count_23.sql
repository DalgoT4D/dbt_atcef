{{ config(
  materialized='table'
) }}

WITH cte AS (
    SELECT
        *
    FROM
       {{ source('source_atecf_surveys', 'subjects_2023') }}
    WHERE
        "Subject_type" = 'Farmer'
        AND "Voided" = 'False'
),
ctenext AS (
    SELECT
        "ID" AS uid,
        observations ->> 'First name' AS first_name,
        LOCATION ->> 'Dam' AS dam,
        LOCATION ->> 'District' AS district,
        LOCATION ->> 'State' AS state,
        LOCATION ->> 'Taluka' AS taluka,
        LOCATION ->> 'GP/Village' AS village,
        observations ->> 'Type of Machine' AS type_of_machine,
        observations ->> 'Category of farmer' AS category_of_farmer,
        observations -> 'Mobile Number' ->> 'phoneNumber' AS mobile_number,
        (observations -> 'Mobile Number' ->> 'verified')::boolean AS mobile_verified,
        CAST("Registration_date" AS date) AS date_time
    FROM
        cte
),

approved_subjects AS (
    SELECT r.*
    FROM ctenext r
    JOIN {{ ref('approval_statuses_niti_2023') }} a 
    ON r.uid = a.entity_id
    WHERE a.entity_type = 'Subject' AND a.approval_status = 'Approved'
),

onem as (
SELECT DISTINCT uid,
        date_time,
        first_name,
        mobile_number,
        mobile_verified,
        state,
        district,
        taluka,
        village,
        dam,
        category_of_farmer
FROM approved_subjects
where category_of_farmer is not null 
), 

encounterjoin AS (
    SELECT 
        m.*
    FROM onem m 
    INNER JOIN {{ ref('encounter_2023') }} e
    ON m.uid = e.farmer_sub_id
) 

SELECT DISTINCT uid, 
    date_time,
    first_name,
    mobile_number,
    mobile_verified,
    state,
    district,
    taluka,
    village,
    dam,
    category_of_farmer
FROM encounterjoin

