with

source as (

    select *
    from {{ source('kaggle', 'raw_races') }}
    
),

renamed as (

    select
        safe_cast(raceId as int) as race_id
        ,safe_cast(name as string) as race_name
        ,safe_cast(date as date) as race_date
        ,safe_cast(year as int) as year

        ,safe_cast(circuitId as int) as circuit_id
        ,safe_cast(round as int) as round

        ,safe_cast(url as string) as race_url
    from
        source

)

select *
from renamed

