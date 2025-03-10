{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gdgs_2023", "gdgs_2023", "gdgs"]
) }}


WITH cte AS (
    SELECT
        "ID" AS uid,
        (
            observations -> 'Mobile Number' ->> 'verified'
        )::boolean AS mobile_verified,
        (
            COALESCE(observations ->> 'Silt to be excavated as per plan', '0')
        )::numeric AS silt_to_be_excavated_as_per_plan,
        (
            COALESCE(observations ->> 'Total Silt Required', '0')
        )::numeric AS total_silt_required,
        (
            COALESCE(observations ->> 'Total Silt Excavated', '0')
        )::numeric AS total_silt_excavated,
        (
            COALESCE(observations ->> 'Farmer contribution per trolley', '0')
        )::numeric AS farmer_contribution_per_trolley,
        (
            COALESCE(observations ->> 'Number of trolleys required', '0')
        )::numeric AS number_of_trolleys_required,
        (
            COALESCE(observations ->> 'Number of hywas required', '0')
        )::numeric AS number_of_hywas_required,
        "Voided" AS subject_voided,
        INITCAP(TRIM(COALESCE(observations ->> 'First name'))) AS first_name,
        INITCAP(COALESCE(location ->> 'Dam')) AS dam,
        INITCAP(COALESCE(location ->> 'District')) AS district,
        CASE  -- Standardize state names
            WHEN
                LOWER(location ->> 'State') LIKE '%maharashtra%'
                THEN 'Maharashtra'
            WHEN
                LOWER(location ->> 'State') LIKE '%maharshatra%'
                THEN 'Maharashtra'
            ELSE INITCAP(COALESCE(location ->> 'State', ''))
        END AS state,
        INITCAP(COALESCE(location ->> 'Taluka')) AS taluka,
        INITCAP(COALESCE(location ->> 'GP/Village')) AS village,
        INITCAP(
            COALESCE(observations ->> 'Type of Machine')
        ) AS type_of_machine,
        INITCAP(
            COALESCE(observations ->> 'Category of farmer')
        ) AS category_of_farmer,
        COALESCE(
            observations -> 'Mobile Number' ->> 'phoneNumber'
        ) AS mobile_number,
        INITCAP(COALESCE(observations ->> 'Name of WB')) AS name_of_wb,
        COALESCE(NULLIF(rwb.stakeholder_responsible, ''), 'Unknown') AS ngo_name
    FROM
        {{ source('source_gdgsom_surveys_2023', 'subjects_2023') }}
    LEFT JOIN
        {{ ref('address_gdgs_23') }} AS rwb
        ON
            location ->> 'Dam' = rwb.dam
    WHERE NOT (LOWER(location ->> 'Dam') ~ 'voided')
),

approval_subjects AS (
    SELECT d.*
    FROM cte AS d
    INNER JOIN
        {{ ref('approval_statuses_gdgs_2023') }} AS a
        ON d.uid = a.entity_id
    WHERE a.entity_type = 'Subject' AND a.approval_status = 'Approved'
)

({{ dbt_utils.deduplicate(
    relation='approval_subjects',
    partition_by='uid',
    order_by='uid desc',
   )
}})
