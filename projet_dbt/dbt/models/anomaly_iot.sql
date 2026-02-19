with base as (
  select
    location,
    window_end,
    avg_temp
  from {{ source('iot', 'iot_agg_1m_5m') }}
),
stats as (
  select
    location,
    avg(avg_temp) as mean_temp,
    stddev_pop(avg_temp) as std_temp
  from base
  group by 1
),
scored as (
  select
    b.location,
    b.window_end,
    b.avg_temp,
    case
      when s.std_temp = 0 then 0
      else (b.avg_temp - s.mean_temp) / s.std_temp
    end as z_score,
    (b.avg_temp >= 30) as is_critical
  from base b
  join stats s using(location)
)
select *
from scored
where abs(z_score) >= 2.5 or is_critical = true
