with

source as (

    select *
    from {{ source('kaggle', 'raw_status') }}
    
),

renamed as (

    select
        safe_cast(statusId as int) as status_id
        ,safe_cast(status as string) as status

    from
        source

)

select *
from renamed

