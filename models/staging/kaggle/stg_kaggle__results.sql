with

source as (

    select *
    from {{ source('kaggle', 'raw_results') }}
    
),

renamed as (

    select
        safe_cast(resultId as int) as result_id
        ,safe_cast(raceId as int) as race_id
        ,safe_cast(driverId as int) as driver_id
        ,safe_cast(constructorId as int) as constructor_id

        ,safe_cast(number as string) as car_number
        ,safe_cast(grid as int) as starting_position
        ,safe_cast(position as int) as final_position
        ,safe_cast(points as numeric) as final_points

        ,safe_cast(time as string) as race_time
        ,safe_cast(milliseconds as int) as race_time_milliseconds

        ,case
            when safe_cast(fastestLapTime as string) != '\\N'
            then time_diff(safe.parse_time('%M:%E3S', fastestLapTime), time '00:00:00', millisecond)
        end as race_fastest_lap_time
        ,safe_cast(fastestLapSpeed as numeric) as race_fastest_lap_speed
        ,safe_cast(statusId as int) as race_status_id

    from
        source

)

select *
from renamed
