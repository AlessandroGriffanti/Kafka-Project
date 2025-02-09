
/* Find the top 5 product categories as for profit */

{{ config(materialized='view') }}

with top_profit_categories as (
    select product_category, sum(profit) as total_profit
    from orders
    group by product_category
)

select product_category, total_profit
from top_profit_categories
order by total_profit DESC
limit 5