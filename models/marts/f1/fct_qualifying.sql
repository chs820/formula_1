with

qualifying as (
    
    select *
    from {{ ref('stg_kaggle__qualifying') }}
    
),

final as (

    select *
    from qualifying

)

select *
from final



