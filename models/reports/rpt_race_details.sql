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

    select *
    from {{ ref('fct_qualifying') }}
    where qualifying_position = 1

),

lap_times as (

    select *
    from {{ ref('fct_lap_times') }}

),

results as (

    select *
    from {{ ref('fct_results') }}

),

race_times as (

    select
        lap_times.race_id
        ,min(lap_times.lap_milliseconds) / 1000 as race_fastest_lap_seconds
        ,min(case when race_position = 1 then lap_times.lap_milliseconds end) / 1000 as race_winner_fastest_lap_seconds
        ,avg(case when race_position = 1 then lap_times.lap_milliseconds end) / 1000 as race_winner_avg_lap_seconds
        ,max(case when race_position = 1 then lap_times.lap_milliseconds end) / 1000 as race_winner_slowest_lap_seconds
        ,sum(case when race_position = 1 then lap_times.lap_milliseconds end) / 1000 as race_winner_total_seconds
    from
        lap_times
    group by 1

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
        ,drivers.driver_name as winning_driver_name
        ,drivers.driver_url as winning_driver_url
        ,constructors.constructor_name as winning_constructor_name
        ,results.starting_position
        ,results.race_time_milliseconds / 1000 as race_time_seconds
        ,results.race_fastest_lap_speed
        ,results.race_status
    from
        results
        left join drivers
            on results.driver_id = drivers.driver_id
        left join constructors
            on results.constructor_id = constructors.constructor_id
    where
        results.final_position = 1
        
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
        ,qualifying_winner_time.q3_milliseconds / 1000 as qualifying_seconds

        ,race_times.race_fastest_lap_seconds
        ,race_times.race_winner_avg_lap_seconds
        ,race_times.race_winner_fastest_lap_seconds
        ,race_times.race_winner_slowest_lap_seconds

        ,race_results.winning_driver_name
        ,race_results.winning_driver_url
        ,race_results.winning_constructor_name
        ,race_results.starting_position
        ,race_results.race_time_seconds
        ,race_results.race_status

    from
        races
        left join circuits
            on races.circuit_id = circuits.circuit_id
        left join qualifying_winner_time
            on races.race_id = qualifying_winner_time.race_id
        left join race_times
            on races.race_id = race_times.race_id
        left join race_results
            on races.race_id = race_results.race_id

)

select *
from final
