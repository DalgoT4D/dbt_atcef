{{ config(
    materialized='table',
    tags=['glific', 'intermediate', '2026']
) }}

with deduplicated_contacts as (
    {{
        dbt_utils.deduplicate(
            relation=source('staging_glific', 'contacts_stg'),
            partition_by='id',
            order_by='cast(updated_at as timestamptz) desc'
        )
    }}
),

raw_field_objects as (
    select
        c.id as contact_id,
        c.phone,
        cast(c.inserted_at as timestamptz)::date as contact_inserted_date,
        cast(c.updated_at as timestamptz)::date as contact_updated_date,
        fields.key as field_name,
        fields.value as field_payload
    from deduplicated_contacts c
    cross join lateral jsonb_each(c.raw_fields::jsonb) as fields(key, value)
    where c.raw_fields is not null
      and c.raw_fields <> '{}'
      and c.phone not like '987654321%'
      and jsonb_typeof(c.raw_fields::jsonb) = 'object'
),

normalized as (
    select
        contact_id,
        phone,
        contact_inserted_date,
        contact_updated_date,
        field_name,
        field_payload ->> 'label' as field_label,
        nullif(btrim(field_payload ->> 'value'), '') as field_value,
        cast(nullif(field_payload ->> 'inserted_at', '') as timestamptz) as field_inserted_at,
        cast(nullif(field_payload ->> 'inserted_at', '') as timestamptz)::date as field_inserted_date
    from raw_field_objects
    where jsonb_typeof(field_payload) = 'object'
)

select *
from normalized
