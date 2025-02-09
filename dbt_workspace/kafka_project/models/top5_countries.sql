
/* Find the top 5 countries as for profit */

{{ config(materialized='view') }}

with top_profit_countries as (
    select country, sum(profit) as total_profit
    from users as u join orders as o on u.id = o.user_id
    group by country

)

select country, total_profit
from top_profit_countries
order by total_profit DESC
limit 5
