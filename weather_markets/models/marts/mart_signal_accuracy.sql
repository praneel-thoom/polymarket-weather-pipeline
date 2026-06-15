with moves as (
    select
        event_time,
        market_id,
        yes_price,
        price_change_pct,
        price_direction,
        abs(price_change_pct) as abs_change_pct
    from {{ ref('stg_polymarket_events') }}
    where abs(price_change_pct) > 5
)

select
    market_id,
    price_direction,
    count(*) as move_count,
    round(avg(abs_change_pct), 2) as avg_abs_change_pct,
    round(max(abs_change_pct), 2) as max_abs_change_pct,
    round(min(yes_price), 4) as min_price_observed,
    round(max(yes_price), 4) as max_price_observed
from moves
group by market_id, price_direction
order by move_count desc
