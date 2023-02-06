with

constructors as (
    
    select *
    from {{ ref('stg_kaggle__constructors') }}
    
),

final as (

    select *
    from constructors

)

select *
from final


