-- models/marts/fct_crypto_daily_metrics.sql

{{ config(
    materialized='table',
    partition_by={
      "field": "date_day",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["sentiment_label"]
) }}

/* CTE 1: daily_trades
   Aggregates granular Binance trade data into daily metrics (Average Price, Volume, and Count).
*/
with daily_trades as (
    select
        date(occurrence_at) as trade_date,
        avg(btc_price_usd) as avg_trade_price,
        sum(trade_amount) as daily_volume,
        count(*) as trade_count
    from {{ ref('stg_crypto__btcusdt') }}
    group by 1
),

/* CTE 2: daily_sentiment
   Extracts daily Fear & Greed scores and categorical labels.
*/
daily_sentiment as (
    select
        date(observation_at) as sentiment_date,
        sentiment_score,
        sentiment_label
    from {{ ref('stg_crypto__sentiment') }}
),

/* CTE 3: daily_prices
   Captures the daily closing price of the asset for ROI calculations.
*/
daily_prices as (
    select
        date(observation_at) as price_date,
        asset_price_usd as closing_price
    from {{ ref('stg_crypto__prices') }}
),

/* CTE 4: base_metrics
   Joins all sources onto a daily timeline and uses LAG to pull the previous day's price.
*/
base_metrics as (
    select
        p.price_date as date_day,
        p.closing_price,
        t.avg_trade_price,
        t.daily_volume,
        s.sentiment_score,
        s.sentiment_label,
        -- Used to calculate daily price volatility/returns in the next step
        lag(p.closing_price) over (order by p.price_date) as prev_day_price
    from daily_prices p
    left join daily_trades t on p.price_date = t.trade_date
    left join daily_sentiment s on p.price_date = s.sentiment_date
),

/* CTE 5: enhanced_metrics
   Applies Window Functions to generate advanced analytical signals:
   - price_return_pct: Daily volatility
   - sentiment_ma_7d: Smoothed sentiment trend (7-day Moving Average)
   - price_7d_later: Used for backtesting predictive signals (LEAD function)
*/
enhanced_metrics as (
    select
        *,
        -- Daily percentage change in price
        (closing_price - prev_day_price) / nullif(prev_day_price, 0) as price_return_pct,
        
        -- Smoothing sentiment to identify long-term shifts in market psychology
        avg(sentiment_score) over (
            order by date_day 
            rows between 6 preceding and current row
        ) as sentiment_ma_7d,
        
        -- Looking forward 7 days to evaluate if current sentiment predicted future price
        lead(closing_price, 7) over (order by date_day) as price_7d_later
    from base_metrics
)

/* Final Select:
   Calculates the 'forward_7d_return' which is the primary metric for our 
   'Buy the Fear' analysis in the Looker Studio dashboard.
*/
select
    *,
    (price_7d_later - closing_price) / nullif(closing_price, 0) as forward_7d_return
from enhanced_metrics