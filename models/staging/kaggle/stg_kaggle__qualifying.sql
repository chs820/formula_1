with

source as (

    select *
    from {{ source('kaggle', 'raw_qualifying') }}
    
),

renamed as (

    select
        safe_cast(qualifyId as int) as qualifying_id
        ,safe_cast(raceId as int) as race_id
        ,safe_cast(driverId as int) as driver_id
        ,safe_cast(constructorId as int) as constructor_id

        ,safe_cast(number as int) as car_number
        ,safe_cast(position as int) as qualifying_position
        ,case
            when safe_cast(q1 as string) != '\\N'
            then safe_cast(q1 as string) 
        end as q1_time
        ,case
            when safe_cast(q2 as string) != '\\N'
            then safe_cast(q2 as string) 
        end as q2_time
        ,case
            when safe_cast(q3 as string) != '\\N'
            then safe_cast(q3 as string) 
        end as q3_time
        ,case
            when safe_cast(q1 as string) != '\\N'
            then time_diff(safe.parse_time('%M:%E3S', q1), time '00:00:00', millisecond)
        end as q1_milliseconds
        ,case
            when safe_cast(q2 as string) != '\\N'
            then time_diff(safe.parse_time('%M:%E3S', q2), time '00:00:00', millisecond)
        end as q2_milliseconds
        ,case
            when safe_cast(q3 as string) != '\\N'
            then time_diff(safe.parse_time('%M:%E3S', q3), time '00:00:00', millisecond)
        end as q3_milliseconds

    from
        source

)

select *
from renamed
