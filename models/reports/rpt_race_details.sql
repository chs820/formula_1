with

races as (

    select *
    from {{ ref('fct_races') }}

),

circuits as (

    select *
    from {{ ref('dim_circuits') }}

),

final as (

    select
        races.races_id
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
    from
        races
        left join circuits
            on races.circuit_id = circuits.circuit_id

)

select *
from final
