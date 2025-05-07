{{ config(
  materialized='table',
  tags=["intermediate","intermediate_niti_2025", "niti_2025", "niti"]
) }}


WITH mycte AS (
    SELECT
        "ID" AS machine_id,
        "Subject_type" AS subject_type,
        "Voided" AS machine_voided,
        INITCAP(TRIM(COALESCE(observations ->> 'First name'))) AS machine_name,
        observations ->> 'Type of Machine' AS type_of_machine,
        INITCAP(COALESCE(location ->> 'Dam')) AS dam,
        INITCAP(COALESCE(location ->> 'District')) AS district,
        CASE
            WHEN
                LOWER(location ->> 'State') LIKE '%maharashtra%'
                THEN 'Maharashtra'
            WHEN
                LOWER(location ->> 'State') LIKE '%maharshatra%'
                THEN 'Maharashtra'
            ELSE INITCAP(COALESCE(location ->> 'State', ''))
        END AS state,
        INITCAP(COALESCE(location ->> 'Taluka')) AS taluka,
        INITCAP(COALESCE(location ->> 'GP/Village')) AS village
    FROM
        {{ source('rwb_niti_2025', 'subjects_niti_2025') }}
    WHERE
        "Subject_type" = 'Excavating Machine'
        AND "Voided" = 'False'
        AND NOT (LOWER(location ->> 'Dam') ~ 'voided')
)

    {{ dbt_utils.deduplicate(
        relation='mycte',
        partition_by='machine_id',
        order_by='machine_id desc'
    ) }}
