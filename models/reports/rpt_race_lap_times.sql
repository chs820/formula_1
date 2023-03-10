with

lap_times as (

    select *
    from {{ ref('fct_lap_times') }}

),

qualifying_times as (

    select *
    from {{ ref('fct_qualifying') }}

),

races as (

    select *
    from {{ ref('fct_races') }}

),

circuits as (

    select *
    from {{ ref('dim_circuits') }}

),

drivers as (

    select *
    from {{ ref('dim_drivers') }}

),

constructors as (

    select *
    from {{ ref('dim_constructors') }}

),

final as (

    select
        races.race_id
        ,races.race_name
        ,races.race_date
        ,races.year
        ,races.race_url

        ,drivers.driver_id
        ,drivers.driver_name
        ,drivers.driver_url

        ,lap_times.lap_number
        ,lap_times.lap_milliseconds / 1000 as lap_seconds

        ,case
            when q3_milliseconds is not null then q3_milliseconds / 1000
            when q2_milliseconds is not null then q2_milliseconds / 1000
            when q1_milliseconds is not null then q1_milliseconds / 1000
        end as qualifying_seconds
        ,qualifying_times.qualifying_position

        ,circuits.circuit_name
        ,circuits.circuit_location_name
        ,circuits.circuit_country_name
        ,circuits.circuit_latitude
        ,circuits.circuit_longitude
        ,circuits.circuit_altitude

    from
        lap_times
        inner join qualifying_times
            on lap_times.race_id = qualifying_times.race_id
            and lap_times.driver_id = qualifying_times.driver_id
        inner join races
            on lap_times.race_id = races.race_id
        inner join circuits
            on races.circuit_id = circuits.circuit_id
        inner join drivers
            on lap_times.driver_id = drivers.driver_id

)

select *
from final
