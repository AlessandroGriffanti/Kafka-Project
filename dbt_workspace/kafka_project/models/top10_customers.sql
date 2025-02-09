
/* Find the top 10 customers as for profit */

{{ config(materialized='view') }}

with top_10_customers as ( 
    select u.firstname as Name, u.lastname as Surname, sum(o.profit) as total_profit 
    from users as u join orders as o on u.id = o.user_id
    group by u.id, u.firstname, u.lastname
)

select Name, Surname, total_profit as Profit
from top_10_customers
order by total_profit DESC
limit 10

