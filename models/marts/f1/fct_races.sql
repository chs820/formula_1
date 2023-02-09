with

races as (
    
    select *
    from {{ ref('stg_kaggle__races') }}
    
),

final as (

    select *
    from races

)

select *
from final
