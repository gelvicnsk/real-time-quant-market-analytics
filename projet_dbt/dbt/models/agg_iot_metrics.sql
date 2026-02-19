select
    event_minute,
    count(*)                       as events_count,
    avg(temperature)               as avg_temperature,
    max(temperature)               as max_temperature,
    min(temperature)               as min_temperature,
    avg(humidity)                  as avg_humidity,
    sum(case when temperature > 70 then 1 else 0 end) as critical_events
from {{ ref('stg_iot') }}
group by event_minute
order by event_minute

