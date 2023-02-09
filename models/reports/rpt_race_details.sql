with

races as (

    select *
    from {{ ref('fct_races') }}

),

circuits as (

    select *
    from {{ ref('dim_circuits') }}

),

qualifying_winner_time as (

    select race_id, q3_time
    from {{ ref('fct_qualifying') }}
    where position = 1

),

final as (

    select
        races.race_id
        ,races.race_name
        ,races.race_date
        ,races.year
        ,races.race_url
        ,circuits.circuit_name
        ,circuits.circuit_location_name
        ,circuits.circuit_country_name
        ,circuits.circuit_latitude
        ,circuits.circuit_longitude
        ,circuits.circuit_altitude
        ,qualifying_winner_time.q3_time as qualifying_time
    from
        races
        left join circuits
            on races.circuit_id = circuits.circuit_id
        left join qualifying_winner_time
            on races.race_id = qualifying_winner_time.race_id

)

select *
from final
