-- models/staging/stg_crypto__prices.sql

{{ config(materialized='view') }}

with source as (
    select * from {{ source('binance_source', 'market_prices') }}
)

select
    safe_cast(timestamp as timestamp) as observation_at,
    cast(price as numeric) as asset_price_usd,
    'BTC' as ticker_symbol,
    'CoinGecko' as data_provider
from source