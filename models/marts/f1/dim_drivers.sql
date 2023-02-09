with

drivers as (
    
    select *
    from {{ ref('stg_kaggle__drivers') }}
    
),

final as (

    select *
    from drivers

)

select *
from final
order by 1
