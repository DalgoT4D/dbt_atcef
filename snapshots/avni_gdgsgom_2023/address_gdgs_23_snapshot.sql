{% snapshot address_gdgs_23_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='address_id',
        strategy='check',
        check_cols=['dam', 'silt_target', 'stakeholder_responsible', 'taluka', 'village', 'district', 'state']
    )
}}

SELECT
    "ID" AS address_id,
    "Title" AS dam,
    "customProperties"->>'Estimated quantity of Silt' AS silt_target,
    "customProperties"->>'Stakeholder responsible' AS stakeholder_responsible,
    "Parent"->>'Title' AS taluka,
    "Parent"->'Parent'->>'Title' AS village,
    "Parent"->'Parent'->'Parent'->>'Title' AS district,
    "Parent"->'Parent'->'Parent'->'Parent'->>'Title' AS state
FROM
    {{ source('source_gdgsom_surveys_2023', 'address_gdgs_2023') }}
WHERE
    "Title" NOT LIKE '%(voided~%)'
{% endsnapshot %}