with

races as (

    select *
    from {{ ref('fct_races') }}

),

circuits as (

    select *
    from {{ ref('dim_circuits') }}

),

results as (

    select *
    from {{ ref('fct_results') }}

),

drivers as (

    select *
    from {{ ref('dim_drivers') }}

),

constructors as (

    select *
    from {{ ref('dim_constructors') }}

),

race_results as (

    select
        results.race_id
        ,drivers.driver_name
        ,drivers.driver_url
        ,constructors.constructor_name
        ,results.starting_position
        ,results.final_position
        ,results.race_time_milliseconds / 1000 as race_time_seconds
        ,results.race_fastest_lap_speed
        ,results.race_status
    from
        results
        left join drivers
            on results.driver_id = drivers.driver_id
        left join constructors
            on results.constructor_id = constructors.constructor_id
        
),

current_constructors as (

    select
        distinct constructor_name
    from
        race_results
        inner join races
            on race_results.race_id = races.race_id
    where
        races.year = 2022

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

        ,race_results.driver_name
        ,race_results.driver_url
        ,race_results.constructor_name
        ,case when current_constructors.constructor_name is not null then TRUE else FALSE end as is_current_constructor
        ,race_results.starting_position
        ,race_results.final_position
        ,race_results.race_time_seconds
        ,race_results.race_status

    from
        races
        left join circuits
            on races.circuit_id = circuits.circuit_id
        left join race_results
            on races.race_id = race_results.race_id
        left join current_constructors
            on race_results.constructor_name = current_constructors.constructor_name

)

select *
from final

