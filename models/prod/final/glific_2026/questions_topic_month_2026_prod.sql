{{ config(
    materialized='table',
    tags=['glific', 'prod', '2026']
) }}

with question_flows as (
    select
        id as flow_result_id,
        contact_phone,
        coalesce(
            cast(nullif(bq_inserted_at, '') as timestamptz),
            cast(inserted_at as timestamptz)
        ) as flow_result_at,
        results::jsonb as results_json
    from {{ source('staging_glific', 'flow_results_stg') }}
    where name = 'More questions'
      and results is not null
      and results <> ''
      and contact_phone is not null
      and contact_phone not like '987654321%'
),

extracted as (
    select
        flow_result_id,
        contact_phone,
        coalesce(
            cast(nullif(results_json #>> '{parent,question,inserted_at}', '') as timestamptz),
            flow_result_at
        ) as question_at,
        nullif(btrim(results_json #>> '{parent,question,input}'), '') as question_input,
        case
            when (results_json -> 'parent') ? 'crpflowsatisfaction' then 'CRP'
            when (results_json -> 'parent') ? 'approvingauthorityflowsatisfaction' then 'Approving Authority'
            else 'Unknown'
        end as result_name
    from question_flows
),

classified as (
    select
        flow_result_id,
        contact_phone,
        question_at,
        question_input,
        result_name,
        case
            when lower(regexp_replace(question_input, '\s+', ' ', 'g')) in ('english', 'marathi', 'hindi', 'help', 'hi', 'hii', 'start training', 'no question', 'no', '4') then null
            when lower(regexp_replace(question_input, '\s+', ' ', 'g')) ~ '^[0-9]+$' then null
            when lower(regexp_replace(question_input, '\s+', ' ', 'g')) ~ '^[0-9]{10,}$' then null
            when lower(regexp_replace(question_input, '\s+', ' ', 'g')) like 'http%' then null
            when lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%user id%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%userid%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%username%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%login%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%password%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%pasward%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%passward%'
              or (
                    lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%avni%'
                and (
                        lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%id%'
                     or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%user%'
                     or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%name%'
                     or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%passw%'
                )
              )
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%otp%' then 'Login / User ID / Password'
            when lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%approving authority%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%अधिकृत प्राधिकरण%' then 'Approving Authority'
            when lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%crp%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%सी. आर. पी%' then 'CRP'
            when lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%video%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%youtube%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%व्हिडिओ%' then 'Training Videos'
            when lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%work order%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%वर्क ऑर्डर%' then 'Work Order'
            when lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%farmer%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%किसान%' then 'Farmer'
            when lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%machine%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%excavat%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%मशीन%' then 'Excavating Machine'
            when lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%daily recording%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%recording%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%रेकॉर्ड%' then 'Daily Recordings'
            when lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%endline%' then 'Endline'
            when lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%language%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%lagvej%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%lang%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%marathi%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%hindi%' then 'Training Language'
            when lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%download%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%open%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%refresh%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%app%' then 'App Access'
            when lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%training%'
              or lower(regexp_replace(question_input, '\s+', ' ', 'g')) like '%प्रशिक्षण%' then 'Training'
            else initcap(question_input)
        end as question_topic
    from extracted
    where question_at >= timestamp '2026-03-01'
      and question_at < timestamp '2027-03-01'
      and question_input is not null
)

select
    date_trunc('month', question_at)::date as month_start,
    (date_trunc('month', question_at) + interval '1 month')::date as month_end,
    2026 as reporting_year,
    date '2026-03-01' as reporting_year_start,
    date '2027-03-01' as reporting_year_end,
    question_topic,
    count(flow_result_id) as question_count
from classified
where question_topic is not null
group by 1, 2, 3, 4, 5, 6
