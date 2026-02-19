with source as (

    select
        sensor_id,
        temperature,
        humidity,
        to_timestamp(event_time) as event_ts
    from {{ source('iot', 'raw_iot') }}

)

select
    sensor_id,
    temperature,
    humidity,
    event_ts,
    date_trunc('minute', event_ts) as event_minute
from source

