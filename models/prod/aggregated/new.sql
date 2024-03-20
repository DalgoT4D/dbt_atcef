with cte as (
    select 
    dam,
    district,
    coalesce(sum(total_silt_carted), 0) as silt_achieved,
    coalesce(max(silt_to_be_excavated), 0) as silt_target
    from   {{ ref('work_order_2023') }}  
    where dam is not null
    group by dam, district
),

ordered_visits AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY dam ORDER BY date_time DESC) AS ov 
  FROM {{ ref('work_order_2023') }}  where date_time is not null
)

select cte.*, ordered_visits.date_time, ordered_visits.silt_to_be_excavated from cte left join ordered_visits 
on cte.dam = ordered_visits.dam and ov = 1

