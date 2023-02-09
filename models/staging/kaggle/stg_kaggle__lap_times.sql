with

source as (

    select *
    from {{ source('kaggle', 'raw_lap_times') }}
    
),

renamed as (

    select
        safe_cast(raceId as int) as race_id
        ,safe_cast(driverId as int) as driver_id

        ,safe_cast(lap as int) as lap_number
        ,safe_cast(position as int) as race_position

        ,safe_cast(time as string) as lap_time
        ,safe_cast(milliseconds as int) as lap_milliseconds

    from
        source

)

select *
from renamed
