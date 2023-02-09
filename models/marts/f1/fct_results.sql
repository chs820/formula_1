with

results as (
    
    select *
    from {{ ref('stg_kaggle__results') }}
    
),

status as (

    select *
    from {{ ref('stg_kaggle__status') }}

),

final as (

    select
        results.*
        ,status.status as race_status
    from
        results
        left join status
            on results.race_status_id = status.status_id

)

select *
from final

