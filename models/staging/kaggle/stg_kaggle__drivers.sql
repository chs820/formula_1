with

source as (

    select *
    from {{ source('kaggle', 'raw_drivers') }}
    
),

renamed as (

    select
        safe_cast(driverId as int) as driver_id,
        safe_cast(driverRef as string) as driver_slug,
        safe_cast(number as int) as driver_number,
        case
            when safe_cast(code as string) != '\\N'
            then safe_cast(code as string) 
        end as driver_code,

        safe_cast(forename as string) as first_name,
        safe_cast(surname as string) as last_name,
        safe_cast(dob as date) as date_of_birth,
        safe_cast(nationality as string) as nationality,
        safe_cast(url as string) as driver_url
    from
        source

)

select *
from renamed

