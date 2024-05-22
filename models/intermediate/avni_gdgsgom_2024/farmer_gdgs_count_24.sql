{{ config(
  materialized='table'
) }}

WITH cte AS (
    SELECT
        *
    FROM
       {{ source('source_gdgsom_surveys', 'subjects_2024') }}
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
    JOIN {{ ref('approval_statuses_gdgs_24') }} a 
    ON r.uid = a.entity_id
    WHERE a.entity_type = 'Subject' AND a.approval_status = 'Approved'
),

onem AS (
    SELECT DISTINCT 
        uid,
        date_time,
        first_name,
        mobile_number,
        mobile_verified,
        state,
        district,
        taluka,
        village,
        dam,
        TRIM(category_of_farmer) AS category_of_farmer
    FROM approved_subjects
),

encounterjoin AS (
    SELECT 
        m.*
    FROM onem m 
    INNER JOIN {{ ref('encounters_2024') }} e
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
