with

source as (

    select *
    from {{ source('kaggle', 'raw_circuits') }}
    
),

renamed as (

    select
        safe_cast(circuitId as int) as circuit_id
        ,safe_cast(circuitRef as string) as circuit_slug
        ,safe_cast(name as string) as circuit_name
        ,safe_cast(location as string) as circuit_location_name
        ,safe_cast(country as string) as circuit_country_name
        ,safe_cast(lat as numeric) as circuit_latitude
        ,safe_cast(lng as numeric) as circuit_longitude
        ,safe_cast(alt as string) as circuit_altitude
        ,safe_cast(url as string) as circuit_url
    from
        source

)

select *
from renamed
