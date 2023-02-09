with

drivers as (
    
    select *
    from {{ ref('stg_kaggle__drivers') }}
    
),

final as (

    select
        *
        ,drivers.first_name || ' ' || drivers.last_name as driver_name
    from
        drivers

)

select *
from final
order by 1
