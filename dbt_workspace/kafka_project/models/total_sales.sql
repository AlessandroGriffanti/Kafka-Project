
/* Find the total sales */

{{ config(materialized='view') }}


select sum(sales) as Total_Sales
from orders

