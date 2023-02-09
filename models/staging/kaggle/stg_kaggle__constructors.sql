with

source as (

    select *
    from {{ source('kaggle', 'raw_constructors') }}
    
),

renamed as (

    select
        safe_cast(constructorId as int) as constructor_id
        ,safe_cast(constructorRef as string) as constructor_slug
        ,safe_cast(name as string) as constructor_name
        ,safe_cast(nationality as string) as constructor_nationality
        ,safe_cast(url as string) as constructor_url
    from
        source

)

select *
from renamed


