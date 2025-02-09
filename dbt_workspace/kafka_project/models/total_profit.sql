
/* Find the total profit */

{{ config(materialized='view') }}


select sum(profit) as Total_Profit
from orders

