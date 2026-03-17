-- models/staging/stg_crypto__btcusdt.sql

{{ config(materialized='view') }}

with source as (
    select * from {{ source('binance_source', 'BTCUSDT') }}
),

renamed as (
    select
        safe_cast(event_time as timestamp) as occurrence_at,
        cast(price as numeric) as btc_price_usd,
        cast(quantity as numeric) as trade_amount,
        is_buyer_maker,
        'BTC' as base_currency,
        'USDT' as quote_currency,
        'Binance' as data_source

    from source
    where price > 0
)

select * from renamed
