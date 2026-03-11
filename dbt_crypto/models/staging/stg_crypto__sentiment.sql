-- models/staging/stg_crypto__sentiment.sql
{{ config(materialized='view') }}

with source as (
    select * from {{ source('binance_source', 'market_sentiment') }}
)

select
    safe_cast(timestamp as timestamp) as observation_at,
    cast(value as int64) as sentiment_score,
    trim(value_classification) as sentiment_label,
    'Alternative.me' as data_provider
from source