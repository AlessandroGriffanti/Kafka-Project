


/* Find the correlation between quantity sold and profit generated for each category of products -> scatterplot */

{{ config(materialized='view') }}

with profit_vs_quantity_category as ( 
    select product_category, sum(quantity) as total_quantity, sum(profit) as total_profit
    from orders
    group by product_category 
)

select product_category, total_quantity as quantity , total_profit as profit
from profit_vs_quantity_category
