with

laps as (
    
    select *
    from {{ ref('stg_kaggle__lap_times') }}
    
),

final as (

    select *
    from laps

)

select *
from final




