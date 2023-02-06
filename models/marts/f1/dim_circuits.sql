with

circuits as (
    
    select *
    from {{ ref('stg_kaggle__circuits') }}
    
),

final as (

    select *
    from circuits

)

select *
from final
