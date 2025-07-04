with 

source as (

    select * from {{ source('raw', 'ship') }}

),

renamed as (

    select
        orders_id,
        shipping_fee,
        shipping_fee_1,
        logcost AS log_cost,
        cast(ship_cost as FLOAT64) as ship_cost,

    from source

)

select * from renamed
